# This file explains how the data in t10.md was obtained.

## Normal voting examples

The earliest example (by block number) was taken from the parallel report (t27a report).

Latest was taken from Kleioscan: screenshot is provided as `normal.png`,
and the corresponding csv file (taken from the corresponding html
document, taken from the site) is specified as `normal.csv`.
Then the data were converted using script from t27a report.

## Voting under attack data

The "attack" took substantial time, but the data are very uniform.
These are mostly full praos blocks, almost all of them having
436 transactions (with slight variations). All we need to know
is how many blocks were found (latest block minus earliest),
and how many slots were used for that (same simple computation). 
Then remaining ~30 different entries were entered manually.

The data were taken from Kleioscan site, and then collected into 
`no-votes-stats.ods` and aggregated using standard LibreOffice 
instruments.

## Experimentation 2026.11.12

Full centrifuge 1hr session, and PIR0 with 9.52% had its BLS keys 
disabled (so that average voting dropped by 9.52% and only two
EBs vere certified with 75% stake). The tx backlog was produced 
for 40 more minutes (total experimentation took 1 hr 40 mins).

Second experiment is next centrifuge session (clean). The data
were taken from Kleioscan site. The Kleioscan format updated, 
so the script was updated too (see `compute_attack_stats_nov.py`).

The data were collected into `test-2026-nov-12.ods` into three
sheets, and then converted to `no-votes-stats-nov-12.csv`
(for disabled voting) and to `votes-stats-nov-12.csv` and
`votes-stats-nov-13.csv` (for normal voting). Then the csv 
tables were trimmed by 0-tx blocks (so only 2 leading and 2 
trailing empty blocks were left).
