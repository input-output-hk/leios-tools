#!/usr/bin/env python3
"""Trusted CI orchestration. Never execute code from the pull request."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
SHA = re.compile(r"[0-9a-f]{40}")
MARKER = re.compile(r"<!-- codex-review head=([0-9a-f]{40}) base=([0-9a-f]{40}) -->")
BOT = "github-actions[bot]"


def api(endpoint, payload=None, pages=False):
    args = ["gh", "api", endpoint]
    if pages:
        args += ["--paginate", "--slurp"]
    if payload is not None:
        args += ["--method", "POST", "--input", "-"]
    result = subprocess.run(args, input=json.dumps(payload) if payload is not None else None,
                            text=True, capture_output=True, check=True)
    data = json.loads(result.stdout)
    return [item for page in data for item in page] if pages else data


def endpoint(suffix):
    return f"repos/{os.environ['GITHUB_REPOSITORY']}/{suffix}"


def output(**values):
    with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
        for key, value in values.items():
            stream.write(f"{key}={value}\n")


def status(head, state, description):
    api(endpoint(f"statuses/{head}"), {
        "state": state, "context": "codex-review", "description": description,
        "target_url": f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}",
    })


def gate():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    requested = os.environ["GITHUB_EVENT_NAME"] == "issue_comment"
    if event["sender"]["type"] == "Bot":
        return
    if requested:
        if not event.get("issue", {}).get("pull_request") or not re.match(
                r"^@codex review(?:\s|$)", event["comment"]["body"]):
            return
        number = int(event["issue"]["number"])
    else:
        number = int(event["pull_request"]["number"])
    # Check both the original actor and the re-run actor before spending API credit.
    for actor in {os.environ["GITHUB_ACTOR"], os.environ["GITHUB_TRIGGERING_ACTOR"]}:
        permission = api(endpoint(f"collaborators/{actor}/permission"))["permission"]
        if permission not in {"admin", "write", "maintain"}:
            print("::notice::Codex review requires repository write access")
            return
    pr = api(endpoint(f"pulls/{number}"))
    if pr["state"] != "open" or (not requested and (pr["draft"] or
            pr["head"]["repo"]["full_name"] != os.environ["GITHUB_REPOSITORY"])):
        return
    head, base = pr["head"]["sha"], pr["base"]["sha"]
    if not SHA.fullmatch(head) or not SHA.fullmatch(base):
        raise ValueError("Invalid commit SHA")
    output(pr=number, head=head, base=base)
    if os.environ["HAS_KEY"] != "true":
        raise RuntimeError("Set the OPENAI_API_KEY Actions secret before enabling Codex review")
    output(ready="true", requested=str(requested).lower())
    status(head, "pending", "Codex review is queued")
    if requested:
        # Acknowledgment is cosmetic. PR comment reactions can be denied even
        # when the token can read the PR and publish its commit status.
        try:
            api(endpoint(f"issues/comments/{int(event['comment']['id'])}/reactions"), {"content": "eyes"})
        except subprocess.CalledProcessError:
            print("::warning::Could not acknowledge the request; continuing the review")


def git(*args, check=True):
    return subprocess.run(["git", "-C", "source", "-c", "core.hooksPath=/dev/null",
                           "-c", "core.fsmonitor=false", *args],
                          text=True, capture_output=True, check=check)


def previous_head(reviews, base):
    for review in reversed(reviews):
        if review.get("user", {}).get("login") == BOT:
            match = MARKER.search(review.get("body") or "")
            if match and match[2] == base:
                return match[1]
    return None


def prepare():
    number, head, base = os.environ["PR"], os.environ["HEAD"], os.environ["BASE"]
    pr = api(endpoint(f"pulls/{number}"))
    if pr["state"] != "open" or pr["head"]["sha"] != head or pr["base"]["sha"] != base:
        output(state="stale")
        return
    reviews = api(endpoint(f"pulls/{number}/reviews?per_page=100"), pages=True)
    comments = api(endpoint(f"pulls/{number}/comments?per_page=100"), pages=True)
    discussion = api(endpoint(f"issues/{number}/comments?per_page=100"), pages=True)
    last = previous_head(reviews, base)
    requested = os.environ["REQUESTED"] == "true"
    if not requested and last == head:
        output(state="already-reviewed")
        return
    exclusions = json.loads((HERE / "exclude.json").read_text())
    pathspecs = ["--", ".", *exclusions]
    diff_args = ["diff", "--no-ext-diff", "--no-textconv", "--no-color"]
    diff = git(*diff_args, f"{base}...{head}", *pathspecs).stdout
    work = Path("review-input")
    work.mkdir(exist_ok=True)
    (work / "pr.diff").write_text(diff)
    mode = "full"
    if not requested and last and git("merge-base", "--is-ancestor", last, head, check=False).returncode == 0:
        # Re-merge diffs include conflict resolutions, without re-reviewing base merges.
        delta = git("log", "--no-ext-diff", "--no-textconv", "--no-color", "--first-parent",
                    "--diff-merges=remerge", "--format=commit %H%n%B", "-p", f"{last}..{head}", *pathspecs).stdout
        (work / "new-commits.patch").write_text(delta)
        mode = "incremental"
    else:
        (work / "new-commits.patch").write_text("Full review: use pr.diff.\n")
    if sum((work / name).stat().st_size for name in ('pr.diff', 'new-commits.patch')) > 2_000_000:
        raise RuntimeError("Review diff exceeds 2 MB; split this PR or review it manually")
    (work / "pr-claims.md").write_text(json.dumps({
        "title": pr["title"], "body": pr["body"],
        "commits": git("log", "--format=%H%n%B", f"{base}..{head}").stdout,
    }, indent=2))
    (work / "previous-comments.json").write_text(json.dumps({
        "reviews": reviews, "inline_comments": comments, "discussion": discussion,
    }, indent=2))
    (work / "scope.json").write_text(json.dumps({"mode": mode, "head": head, "base": base}))
    output(state="review", mode=mode)


def changed_lines(patch):
    """Return commentable changed lines on both sides of a GitHub file patch."""
    allowed = set()
    old = new = None
    for line in patch.splitlines():
        match = re.match(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
        if match:
            old, new = map(int, match.groups())
        elif old is not None:
            if line.startswith("+"):
                allowed.add(("RIGHT", new))
                new += 1
            elif line.startswith("-"):
                allowed.add(("LEFT", old))
                old += 1
            elif line.startswith(" "):
                old += 1
                new += 1
    return allowed


def validate(result, files):
    if not isinstance(result, dict) or set(result) != {"summary", "findings"}:
        raise ValueError("Review must contain summary and findings")
    def prose(value, limit):
        if not isinstance(value, str) or not value.strip() or len(value.split()) > limit:
            raise ValueError("Review text is empty or too long")
        if re.search(r"\bP[0-3]\b", value, re.I) or "<!--" in value:
            raise ValueError("Use high/medium labels and no hidden markers")
    prose(result["summary"], 100)
    findings = result["findings"]
    if not isinstance(findings, list) or len(findings) > 10:
        raise ValueError("Expected at most ten findings")
    allowed = {f["filename"]: changed_lines(f.get("patch", "")) for f in files}
    comments = []
    for item in findings:
        if not isinstance(item, dict) or set(item) != {"severity", "path", "line", "side", "title", "body"}:
            raise ValueError("Invalid finding fields")
        if item["severity"] not in {"high", "medium"}:
            raise ValueError("Only high and medium findings are accepted")
        if type(item["line"]) is not int or (item["side"], item["line"]) not in allowed.get(item["path"], set()):
            raise ValueError("Finding must target a changed line in the current PR")
        prose(item["title"], 15)
        prose(item["body"], 90)
        comments.append({"path": item["path"], "line": item["line"], "side": item["side"],
                         "body": f"**{item['severity']} — {item['title']}**\n\n{item['body']}"})
    return comments


def publish():
    number, head, base = os.environ["PR"], os.environ["HEAD"], os.environ["BASE"]
    try:
        pr = api(endpoint(f"pulls/{number}"))
        if pr["state"] != "open" or pr["head"]["sha"] != head or pr["base"]["sha"] != base:
            print("::notice::PR changed or closed; discarding stale review")
            status(head, "success", "Review superseded by a PR update")
            return
        if os.environ["REVIEW_RESULT"] != "success":
            raise RuntimeError("Codex review job failed; inspect its Actions log")
        state = os.environ["PREPARE_STATE"]
        if state in {"already-reviewed", "stale"}:
            status(head, "success", "No additional review required")
            return
        if state != "review":
            raise RuntimeError("Review preparation did not complete")
        run_marker = f"<!-- codex-review run={os.environ['GITHUB_RUN_ID']} -->"
        reviews = api(endpoint(f"pulls/{number}/reviews?per_page=100"), pages=True)
        if any(r.get("user", {}).get("login") == BOT and run_marker in (r.get("body") or "") for r in reviews):
            status(head, "success", "Review already posted by this run")
            return
        if os.environ["REQUESTED"] != "true" and previous_head(reviews, base) == head:
            status(head, "success", "This head was already reviewed")
            return
        raw = os.environ["REVIEW_JSON"]
        if len(raw) > 100_000:
            raise ValueError("Review output exceeds 100 KB")
        result = json.loads(raw)
        files = api(endpoint(f"pulls/{number}/files?per_page=100"), pages=True)
        comments = validate(result, files)
        # One atomic review contains the summary, inline comments, and retry marker.
        # No automatic POST retry: a rerun checks the marker after an uncertain response.
        body = (f"Codex {os.environ['REVIEW_MODE']} review: {len(comments)} high/medium finding(s).\n\n"
                f"{result['summary']}\n\nStatic review; tests were not executed.\n\n"
                f"<!-- codex-review head={head} base={base} -->\n{run_marker}")
        current = api(endpoint(f"pulls/{number}"))
        if current["state"] != "open" or current["head"]["sha"] != head or current["base"]["sha"] != base:
            status(head, "success", "Review superseded by a PR update")
            return
        api(endpoint(f"pulls/{number}/reviews"), {
            "commit_id": head, "event": "COMMENT", "body": body, "comments": comments,
        })
        status(head, "success", "Codex review posted; see PR comments")
    except Exception:
        status(head, "failure", "Review failed; inspect the Actions log")
        raise


if __name__ == "__main__":
    {"gate": gate, "prepare": prepare, "publish": publish}[sys.argv[1]]()
