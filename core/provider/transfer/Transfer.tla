---- MODULE Transfer ----
\* A Space transfer: Transfer.Execute in transfer.go and the session routine
\* that runs it, StartTransfer in core/resource/session/transfer.go.
\*
\* The person picks a mode and the Spaces to move. Execute scans the source;
\* Merge and Migrate, which delete the source, refuse a pick that leaves a Space
\* out. It copies each picked Space's blocks, then adds each to the target with
\* its state and checkpoints it. Migrate signs the target Spaces with the
\* source's peer key, so it copies that key to the target next. Merge and
\* Migrate then delete each moved Space and the source volume, which also holds
\* the checkpoint, and the session routine deletes the source session. The
\* routine can stop at any step, by a crash or a failed copy, and the person can
\* start it again; a new run resumes from the checkpoint.

EXTENDS Naturals, FiniteSets

Spaces == {"a", "b"}
Modes == {"merge", "migrate", "mirror"}
\* MaxFaults bounds the crashes and failures in one behavior, so the model is
\* finite and a transfer that stops faulting can finish.
MaxFaults == 1

VARIABLES
  mode,     \* the requested mode
  picked,   \* the Spaces the person picked in the wizard
  pc,       \* the routine's step; "idle" when no routine runs
  entries,  \* the Spaces this run transfers, from its scan
  copied,   \* the Spaces whose blocks this run copied, held in memory
  tgt,      \* the Spaces the target holds, blocks and state
  ckpt,     \* the checkpoint's completed Spaces, stored in the source volume
  src,      \* the Spaces the source volume holds
  vol,      \* the source volume exists
  session,  \* the source session exists
  key,      \* the target holds the source's peer key
  faults
vars == <<mode, picked, pc, entries, copied, tgt, ckpt, src, vol, session, key, faults>>

Deletes == mode \in {"merge", "migrate"}
Ends == {"done", "refused"}

Init ==
  /\ mode \in Modes
  /\ picked \in SUBSET Spaces \ {{}}
  /\ pc = "idle"
  /\ entries = {}
  /\ copied = {}
  /\ tgt = {}
  /\ ckpt = {}
  /\ src = Spaces
  /\ vol = TRUE
  /\ session = TRUE
  /\ key = FALSE
  /\ faults = 0

\* StartTransfer builds the source from the source session's volume, so a run
\* starts only while both exist. Execute reads the checkpoint, then scans.
Start ==
  /\ pc = "idle" /\ session /\ vol
  /\ ~(Deletes /\ src \ picked # {})
  /\ pc' = "blocks"
  /\ entries' = src \cap picked
  /\ copied' = {}
  /\ UNCHANGED <<mode, picked, tgt, ckpt, src, vol, session, key, faults>>

\* Copy one Space's blocks; a Space the checkpoint holds is skipped.
CopyBlocks(s) ==
  /\ pc = "blocks" /\ s \in entries \ ckpt /\ s \notin copied
  /\ copied' = copied \cup {s}
  /\ UNCHANGED <<mode, picked, pc, entries, tgt, ckpt, src, vol, session, key, faults>>

BlocksDone ==
  /\ pc = "blocks" /\ entries \ ckpt \subseteq copied
  /\ pc' = "state"
  /\ UNCHANGED <<mode, picked, entries, copied, tgt, ckpt, src, vol, session, key, faults>>

\* Add one Space to the target with its replayed state, then checkpoint it.
CopyState(s) ==
  /\ pc = "state" /\ s \in entries \ ckpt /\ s \in copied
  /\ tgt' = tgt \cup {s}
  /\ ckpt' = ckpt \cup {s}
  /\ UNCHANGED <<mode, picked, pc, entries, copied, src, vol, session, key, faults>>

StateDone ==
  /\ pc = "state" /\ entries \subseteq ckpt
  /\ pc' = CASE mode = "migrate" -> "key" [] Deletes -> "cleanup" [] OTHER -> "finish"
  /\ UNCHANGED <<mode, picked, entries, copied, tgt, ckpt, src, vol, session, key, faults>>

\* Merge and Migrate refuse to start when the pick leaves out a Space, since
\* deleting the volume would delete it.
Refuse ==
  /\ pc = "idle" /\ session /\ vol
  /\ Deletes /\ src \ picked # {}
  /\ pc' = "refused"
  /\ UNCHANGED <<mode, picked, entries, copied, tgt, ckpt, src, vol, session, key, faults>>

\* Migrate gives the target the source's peer key while the source holds it.
CopyKey ==
  /\ pc = "key"
  /\ key' = TRUE
  /\ pc' = "cleanup"
  /\ UNCHANGED <<mode, picked, entries, copied, tgt, ckpt, src, vol, session, faults>>

\* Cleanup deletes each scanned Space from the source, ignoring failures.
DeleteSpace(s) ==
  /\ pc = "cleanup" /\ s \in entries \cap src
  /\ src' = src \ {s}
  /\ UNCHANGED <<mode, picked, pc, entries, copied, tgt, ckpt, vol, session, key, faults>>

\* Then it deletes the source volume, with every Space and the checkpoint in
\* it; a failure is logged and the transfer goes on.
DeleteVolume ==
  /\ pc = "cleanup"
  /\ \/ vol' = FALSE /\ src' = {} /\ ckpt' = {}
     \/ UNCHANGED <<vol, src, ckpt>>
  /\ pc' = "finish"
  /\ UNCHANGED <<mode, picked, entries, copied, tgt, session, key, faults>>

\* Execute deletes the checkpoint and returns; the session routine then
\* deletes the source session in Merge and Migrate.
Finish ==
  /\ pc = "finish"
  /\ ckpt' = IF vol THEN {} ELSE ckpt
  /\ pc' = IF Deletes THEN "session" ELSE "done"
  /\ UNCHANGED <<mode, picked, entries, copied, tgt, src, vol, session, key, faults>>

DeleteSession ==
  /\ pc = "session"
  /\ session' = FALSE
  /\ pc' = "done"
  /\ UNCHANGED <<mode, picked, entries, copied, tgt, ckpt, src, vol, key, faults>>

\* The routine stops at any step, by a crash or a failed copy, losing what it
\* held in memory.
Fault ==
  /\ pc \notin Ends \cup {"idle"} /\ faults < MaxFaults
  /\ pc' = "idle"
  /\ copied' = {}
  /\ entries' = {}
  /\ faults' = faults + 1
  /\ UNCHANGED <<mode, picked, tgt, ckpt, src, vol, session, key>>

\* A run stopped after deleting the source volume and before deleting the
\* source session: no run can start without the volume, so that session stays
\* until the person removes it.
Stranded == pc = "idle" /\ session /\ ~vol

Done == (pc \in Ends \/ Stranded) /\ UNCHANGED vars

Progress ==
  \/ Start \/ Refuse \/ BlocksDone \/ StateDone \/ DeleteVolume \/ Finish \/ CopyKey \/ DeleteSession
  \/ \E s \in Spaces : CopyBlocks(s) \/ CopyState(s) \/ DeleteSpace(s)
Next == Progress \/ Fault \/ Done
Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

\* Every Space the person had is in the source or on the target.
NoSpaceLost == \A s \in Spaces : s \in src \/ s \in tgt
\* Mirror copies and never deletes.
MirrorKeepsSource == mode = "mirror" => src = Spaces /\ vol /\ session
\* Migrate deletes the source volume only after the target holds its peer key.
MigrateKeepsKey == mode = "migrate" /\ ~vol => key
\* A transfer finishes, is refused, or is stranded.
Finishes == <>(pc \in Ends \/ Stranded)
====
