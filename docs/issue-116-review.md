# Issue #116 — review and handoff

The announcement-ordering fix and both PR #118 review findings were addressed.
Validation completed on 2026-10-28.

| Completed work | Result |
| --- | --- |
| Announcement ordering | Added per-connection send tracking and header replay before dependent body and transaction offers. |
| Deferred offers | Preserved unsent offers through missing headers and temporary eviction, allowing delivery to resume without blocking unrelated notifications. |
| Retention | Bounded retained state and prevented unverified peer slots from advancing expiry. |
| Regression coverage | Verified reconnects, retries, source filtering, duplicate suppression, transaction offers, adversarial controls, cancellation and shutdown. Both review regressions failed before their fixes and passed afterward. |
| Proto-devnet harness | Enabled explicit all-blocks production and corrected the certificate-window calculation. |

| Check | Result |
| --- | --- |
| Networking workspace tests | 467 passed; 1 documentation test ignored. |
| Downstream workspace tests, including net-node | 567 passed; 1 test ignored. |
| Workspace builds and downstream release builds | Passed. |
| Clippy | Passed with existing warnings. |
| Formatting | Failed on pre-existing formatting differences; no new differences in the edited Rust files. |
| Harness checks | 4 Python tests passed; shell syntax and generated configuration checks passed. |

Mixed Rust/Haskell smoke tests used the maintained **proto-devnet** harness
(`make smoke`), three Haskell nodes using `rc-2026w41`, and Rust `net-node`.
Every attempt used a fresh devnet, a fresh 1,200-transaction magazine and the
full 1,800-second measurement window. The first three runs preceded the final
review fixes; the fourth included them.

| Run | Overall result | Ordering errors | Ranking blocks adopted | Endorser-block certificates | Transactions settled |
| --- | --- | --- | --- | --- | --- |
| Earlier 1 | Failed 1 voting gate | 0 | 21/21 | 6 | 347/1,200 |
| Earlier 2 | Failed 2 voting gates | 0 | 22/23 | 6 | 343/1,200 |
| Earlier 3 | Passed all 10 gates | 0 | 19/19 | 8 | 440/1,200 |
| After review fixes | Passed 9 gates; failed 1 voting gate | 0 | 18/18 | 5 | 903/1,200 |

Legend: ordering errors are `ExnLeiosBlockOfferWithoutAnnouncement` exceptions.
Block adoption and certificates refer to Rust-produced blocks observed on the
Haskell chain. Settlement counts magazine transactions applied by the Haskell ledger.

| Additional results after review fixes | Observation |
| --- | --- |
| Failed voting gate | 9/11 Rust endorser blocks received votes (81.8%; required 85%). One contained already-spent transactions; another lost its tip position before voting. |
| Decoding and slot collisions | Zero Haskell decoding failures and zero slot collisions. |
| Other errors observed | 1 startup fetch exception, 31 certified-parent lookup errors and 1 missing-parent-announcement error. These remain separate unresolved findings. |
