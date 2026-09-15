# PR 2 review follow-up

The review fixes change capture reliability and prepare a comparison of two
fanout rules under the same total copy budget. They do not establish that BP
protection restores quorum. That still requires rerunning the bounded arms.

| Finding | Resolution |
|---|---|
| A failed capture leaves the sequential worker running | Validate and open a temporary output before starting; cancel and await the worker when the monitor fails. |
| All-stake topologies silently disable the cap | Stake no longer controls exemptions. Protection requires explicit link markers; an entirely protected topology is rejected. Ordinary all-stake topologies still obey the cap. |
| Protected fanout sends k+1 copies | Protected consumers take places within k; configurations with more protected consumers than places are rejected. Source exclusion also frees a place. |
| The new default changes old overlays | Protection defaults to false in parameters and the runner. |
| The setting is absent from run identity | Overlays, run names, runs.csv, result keys and displayed tables include the protection setting. |
| Stake is an ownership proxy | The topology marks each BP's own upstream entries with always-forward-votes. The study runner verifies its two-relay assumption and saves those markers in both topologies. |
| Consumers are partitioned on every forward | Classify once per node and reuse the ranking buffer. |
| Path aliases can mix event and traffic outputs | Resolve parent directories and symlinks before comparing destinations; existing reports are never overwritten. |
| Partial logs and inflated durations pass | Require final summaries, successful completion and a finite duration matching requested slots. Version 1 archives additionally require their successful runs.csv record. |
| Embedded binary revision is stale | Track common Git refs in linked worktrees; the runner checks the version, rebuilds a stale CLI, and fails if it still differs. Historical metadata is preserved. |
| Missing generation size aborts capture | Receive events carry body bytes directly; no generation lookup is needed. |
| The capture test only plans runs | Exercise command execution and capture checksums, plus a separate real CLI suite. |
| Failed capture blocks retry | Publish the complete temporary file only after successful simulation and monitor completion. Failed/interrupted captures remove their temporary file. |
| Duplicate accounting and integer message kinds | The trace aggregator and traffic recorder share one typed vote-wire classifier. |
| Repeated single-value flag parsing | Use one flag parser. |

The recorded 2026-09-15 executable still has the historical version mismatch
described in its README. A new runner check cannot repair old provenance. Its
raw artifacts remain unchanged, and the current summarizer reproduces both
published tables byte for byte using the original successful run record.

Validation: 166 Rust tests passed, one ignored; 13 runner/extractor tests and
four summarizer tests passed. The real CLI suite covers both engines, push and
pull capture, path aliases, cancellation, monitor failure and retry. The original
108-run extraction still reproduces all published numeric results.

No full study rerun is needed to validate these implementation fixes. The
protected/unprotected fanout comparison remains outstanding. Announcement and
request sizes remain 8-byte model assumptions and need their own parameterized
experiments before quoting a general bandwidth ratio.
