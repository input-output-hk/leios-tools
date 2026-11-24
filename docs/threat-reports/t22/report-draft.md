## Step 1. T22 in fully-controlled environment.

### State up to 18.09.2026

* Some draft is implemented, however net-rs does not have proper EB certification (broken).
  So, no forks are observed.

* Voting spread in net-rs takes less than 1 second. So, no adaptive scheme is possible in 
  this situation.

* Data from simulation: 3.5 secs for 95% quorum. So, 2 secs-2.5 secs delay for 75% quorum.
  In order to emulate this possibility, EB distribution delay is need to be introduced
  (response of EB and Tx requests delay).

  Delay instruments: 
  + network roundtrip (100ms-200ms-300ms realistic values)
  + egress delay (put limit to outbound traffic and regulate it)
  + additional verification delays (?)
  + ask Wolf about reason for this number.

* Control is necessary: voting phase is 7 secs, and later distribution may happen in 
  20-30 seconds.

### Plan for 21.09.2026 and further...

* Develop EB certification, implement hard-linked T22, see forks.

* Include delays to EB delivery: so that centralized control will be possible to test.
