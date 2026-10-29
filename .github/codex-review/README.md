# Codex pull request reviews

This implementation is inactive: Claude reviews have been restored and the
`codex-review` workflow removed. The code and tests remain for a future rollback.
See [Claude review setup](../claude-review.md) for the active configuration.

The historical `codex-review` workflow It posts
suggestions only: one GitHub review with concise inline **high** and **medium**
findings. It never edits code, pushes commits, approves, or requests changes.
`REVIEW.md` contains the repository-specific review criteria.

## Setup

1. Add an OpenAI Platform API key with funded API access as the Actions secret
   `OPENAI_API_KEY` in each repository. A ChatGPT subscription is not an API key.
   Use **Settings → Secrets and variables → Actions**, or run these commands
   locally and enter the key at the hidden prompt (do not put it in a commit,
   command argument, or PR comment):

   ```sh
   gh secret set OPENAI_API_KEY --repo input-output-hk/leios-adversarial-tools
   gh secret set OPENAI_API_KEY --repo input-output-hk/leios-tools
   ```

   Alternatively, use an organization Actions secret restricted to these two
   repositories, if the organization's GitHub plan permits it.
2. Merge the workflow into each repository's default branch. Comment-triggered
   runs use the default-branch workflow. The migration PR itself cannot run the
   new workflow before it is merged.
3. Optionally set the Actions variable `CODEX_REVIEW_MODEL`. The default is
   `gpt-6.1-sol`, with high reasoning effort. The API project must have access to
   the selected model. Model changes do not require editing the workflow.
4. On a small open PR, post `@codex review` as a user with repository write
   access. Expect a `codex-review` commit status and a review from
   `github-actions[bot]`. An eyes reaction is best effort; missing reaction
   permission does not stop the review. Check that inline findings use high/medium.

The repository's Actions policy must permit the pinned `openai/codex-action`
and `actions/checkout` actions. Organization policy must also permit the workflow's
pull-request write permission. Missing credentials or API/model errors fail the
review; they are not reported as a clean review. The workflow has a 30-minute
analysis timeout, but that is not a dollar spending cap; manage API usage in the
OpenAI project. Do not make `codex-review` a required merge check: some PRs are
intentionally skipped, and successful execution is not a correctness approval.

## Behavior

- Non-draft, same-repository PRs are reviewed when opened, reopened, marked ready,
  or updated. The triggering user and anyone rerunning a job need write access.
  Bot events are ignored.
- Subsequent pushes review commits since the last completed Codex review on the
  same base. Force pushes without that ancestor, and changes to the base commit,
  fall back to a full review. Generated files in `exclude.json` are omitted.
- `@codex review` as a top-level PR comment requests a full review, including for
  draft or fork PRs. Outsiders cannot trigger a paid review by commenting.
  This is a CI command; it does not require the Codex GitHub App.
- Existing review comments are provided as context to discourage duplicates.
  Completed heads and retried runs are deduplicated before publishing. An explicit
  request intentionally allows another review of the same head.
- A changed or closed PR is checked before analysis and publication. Stale results
  are discarded. Inline locations must refer to changed lines at the reviewed
  commit. Missing/truncated GitHub patches or malformed model output fail
  validation instead of silently dropping findings.
- No builds or tests run during review. The summary says it is a static review;
  normal build/test workflows remain responsible for execution. Diffs above 2 MB
  require manual review or splitting the PR.

## Trust boundaries and maintenance

`pull_request_target` supplies trusted orchestration from the base, and
`issue_comment` supplies it from the default branch. Every executable script and
prompt is checked out at `github.workflow_sha`, never from the PR head. The head
checkout is isolated as `source/`, used only as data. Do not add steps that execute
PR code, dependency installation, or PR-controlled actions to this workflow.

The analysis job has only read access to GitHub. Codex runs last, with the official
action's protected API proxy, `drop-sudo`, a read-only permission profile, an
isolated Codex home, and automatic project instruction loading disabled. The API
key is an action input, never a job-wide environment variable. A separate job
checks the model's JSON and posts an atomic `COMMENT` review, without an OpenAI
key or a checkout of the PR. Both jobs use fresh GitHub-hosted runners.

The schema, prompt, and publisher enforce high/medium labels and short comments.
A retry checks a marker in the posted review, preventing duplicate comments even
when GitHub accepted a request but its response was lost. Do not automatically
retry review POST requests. Failed runs can be rerun from Actions or retriggered
with a new `@codex review` comment.

Run the local checks without credentials or API spend:

```sh
python3 -m unittest discover -s .github/codex-review -p 'test_*.py'
```

Keep the shared workflow, orchestration, tests, prompt, and schema synchronized
between the two repositories; only `exclude.json` and `REVIEW.md` differ. Update
the pinned action commit and Codex CLI version together after checking the
[official action documentation](https://learn.chatgpt.com/docs/github-action).
