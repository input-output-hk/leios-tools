# Claude pull request reviews

The restored workflow uses `anthropics/claude-code-action` and the Actions secret
`ANTHROPIC_API_KEY`. The current Anthropic Console key is named **RedTeam-CI**;
that display name does not change the GitHub secret name. Update the secret in
both repositories through Settings → Secrets and variables → Actions, or use
`gh secret set ANTHROPIC_API_KEY --repo OWNER/REPO` and enter it at the hidden
prompt. Never put the key in source, command arguments, or comments.

Non-draft same-repository PRs run automatically. Subsequent pushes review changes
since the last completed Claude review. A maintainer can request a full review
with `@claude review` or Actions → claude-review → Run workflow → PR number.
Comment and manual triggers require the workflow on the default branch.

The previous model is `claude-opus-5`; the Actions variable `CLAUDE_REVIEW_MODEL`
can override it. A preflight checks API authentication and model access before
starting Claude. The review itself verifies generation and publication access.
API billing and usage limits belong to the Anthropic organization.

Reviews post concise **high** and **medium** inline findings and one summary.
They read source without executing builds, tests, or edits. `REVIEW.md` defines
the criteria. The workflow pins the previous action version and checks out the
resolved head commit and loads review criteria from the trusted workflow commit.
`pull_request_target` keeps the workflow itself on the base branch, with an
explicit same-repository gate for automatic runs. Explicit settings disable automatic agent
instructions, skills, hooks, and project servers while retaining Read/Grep/Glob; the prompt explicitly names the trusted
review criteria. Diffs above 2 MB fail before analysis; smaller diffs must be
read in chunks when needed. PR content is review data. A failed review must not
be interpreted as a clean correctness review. A successful run is not merge
approval; findings are suggestions for the author to assess.

Codex automatic reviews are disabled; its implementation and tests remain under
`.github/codex-review/` for rollback. Normal project CI remains enabled.
