---- MODULE StableCheckpoint ----
\* The stable checkpoint of a World whose publications are pending, without
\* the page view (checkpoint.go). Each World write admits its state under the
\* provider's writeMtx (core/provider/local sostate.go), which an admission
\* holds only until the publisher admits it, and queues a receipt that settles
\* once its blocks and head are durable. On each state update, under the
\* writer lock, the engine prepares a checkpoint unless one is in flight
\* (stableBusy): nameWorld names the World by the last admitted receipt. The
\* checkpoint routine waits for that receipt (waitNamed), then writes the
\* state through UpdateSOState: the state lock takes writeMtx and waits for
\* every pending receipt (waitSettled) before it adopts the checkpoint and
\* releases writeMtx. A failed receipt fails every receipt after it; a
\* checkpoint waiting on one is dropped.
\*
\* The run starts with up to Queue receipts pending, as after a restart, and
\* at most one failure, so failures stop.
\*
\* Not modeled: the stores behind a receipt (SQLPrimary.tla in
\* core/provider/local), the operations' content, the stale check, which
\* only skips an adoption the held state moved past, group control, and
\* the page view, whose checkpoint names a World in its own publication.

EXTENDS Naturals

\* Queue: the receipts the publisher holds pending at most.
CONSTANTS Queue

VARIABLES
  pend,    \* admitted receipts not yet settled
  nr,      \* the pending receipts up to the named one, 0 once it settled
  ck,      \* the checkpoint: "idle", "named" (in waitNamed), "locked" (holds writeMtx)
  open,    \* a write was admitted after the last named World
  adopted, \* the last named checkpoint was adopted
  fell     \* a receipt failed
vars == <<pend, nr, ck, open, adopted, fell>>

Init ==
  /\ pend \in 0..Queue /\ nr = 0 /\ ck = "idle"
  /\ open = (pend > 0) /\ adopted = FALSE /\ fell = FALSE

\* A World write admits its state under writeMtx, which the checkpoint's
\* state write may hold, and queues its receipt.
Write ==
  /\ ck # "locked" /\ pend < Queue
  /\ pend' = pend + 1 /\ open' = TRUE
  /\ UNCHANGED <<nr, ck, adopted, fell>>

\* The publisher settles the oldest pending receipt.
Settle ==
  /\ pend > 0
  /\ pend' = pend - 1 /\ nr' = IF nr > 0 THEN nr - 1 ELSE 0
  /\ UNCHANGED <<ck, open, adopted, fell>>

\* The oldest pending receipt fails, and every receipt after it. A checkpoint
\* waiting on one of them is dropped; one holding writeMtx sees the durable
\* state, which still holds the World it named.
Fail ==
  /\ pend > 0 /\ ~fell
  /\ pend' = 0 /\ nr' = 0 /\ fell' = TRUE
  /\ ck' = IF ck = "named" /\ nr > 0 THEN "idle" ELSE ck
  /\ UNCHANGED <<open, adopted>>

\* A state update prepares a checkpoint unless one is in flight, naming the
\* World by the last admitted receipt.
Prepare ==
  /\ ck = "idle" /\ open
  /\ ck' = "named" /\ nr' = pend /\ open' = FALSE /\ adopted' = FALSE
  /\ UNCHANGED <<pend, fell>>

\* Once the named receipt settled, the state write takes writeMtx.
Lock ==
  /\ ck = "named" /\ nr = 0
  /\ ck' = "locked"
  /\ UNCHANGED <<pend, nr, open, adopted, fell>>

\* Once every pending receipt settled, it adopts the checkpoint and releases
\* writeMtx.
Adopt ==
  /\ ck = "locked" /\ pend = 0
  /\ ck' = "idle" /\ adopted' = TRUE
  /\ UNCHANGED <<pend, nr, open, fell>>

Next ==
  \/ Write \/ Settle \/ Fail \/ Prepare \/ Lock \/ Adopt
  \/ UNCHANGED vars
Spec == Init /\ [][Next]_vars

\* Fairness: the publisher keeps settling receipts, the engine prepares on
\* each state update, and the checkpoint routine runs. Writes may continue
\* forever and never pause.
FairSpec == Spec /\ WF_vars(Settle) /\ WF_vars(Prepare) /\ WF_vars(Lock)
              /\ WF_vars(Adopt)

\* Every admitted write is named by a checkpoint, even while writes never
\* pause, so the state's operation history stays bounded.
Named == open ~> ~open
\* A named checkpoint is adopted or dropped, so stableBusy clears.
Finishes == (ck # "idle") ~> (ck = "idle")
\* writeMtx is released again, so writes keep being admitted.
Admits == []<>(ck # "locked")

\* No World write waits behind a receipt: the checkpoint holds writeMtx only
\* once nothing is pending. The direct state write breaks this, holding
\* writeMtx while waitSettled waits for the publisher's whole queue, so
\* StableCheckpoint.cfg does not check it. Admitting the checkpoint's state
\* as a publication after the last receipt, as a World write's state is
\* admitted, would keep it.
NoWaitUnderLock == ck = "locked" => pend = 0
====
