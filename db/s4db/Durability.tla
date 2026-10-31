---- MODULE Durability ----
\* Commits, flushes, checkpoints and the release of freed space, as the writer
\* and flusher run them, against every state a crash can leave on the drive.
\*
\* One key is overwritten by each commit, which writes its value into a free
\* run and appends a record carrying the durable mark it was written under.
\* The value it replaced is freed at the commit and released, for reuse, once
\* the flusher's logged mark reaches it (releaseSpace). A flush (syncThrough)
\* makes everything written durable and raises the durable, logged and
\* checkpoint marks from the state it claimed. A checkpoint (saveTree) writes
\* the tree into a free page, flushes with a barrier, writes the superblock
\* and publishes; the page the old tree used is freed at the new checkpoint
\* and released once the checkpoint is durable.
\*
\* A crash keeps the durable superblock or the newer written one, the flushed
\* records and any prefix of the rest, and for each run and page either what
\* was durable or what was written since. Recovery (load, tail with verify)
\* reads the tree the superblock names, takes the highest durable mark among
\* the records after it, and ends the log at the first record past that mark
\* whose value no longer matches.
\*
\* On Linux and Windows the barrier is a durable flush and raises the marks.
\* On darwin it is F_BARRIERFSYNC, which only orders the writes before it
\* ahead of those after, and raises none. So the drive is tracked twice: as a
\* crash that keeps no write after the last barrier may leave it (phys,
\* vdisk, pdisk, from sbDur), and as one that keeps a later write, which
\* then keeps every write before the barrier (physB, vdiskB, pdiskB, from the
\* superblock written before the barrier or the one after it).
\*
\* Compaction needs no action of its own: relocate rewrites values through a
\* commit, which frees their old runs like any commit, and moveLog continues
\* the log in a free chunk, leaving the old chunk to be freed by the next
\* checkpoint like a tree page. Not modeled: pins and other processes
\* (Pins.tla), the log reader's checksum chain, which ends the log at a stale
\* record in a reused chunk, and the space record.

EXTENDS Naturals, Sequences, FiniteSets

MaxSeq == 3
Slots == {1, 2, 3}
Pages == {"p1", "p2"}
Empty == 9
NoTree == [slot |-> 0, tag |-> Empty]

VARIABLES
  platform,    \* "linux": the barrier is durable; "darwin": it orders only
  seq,         \* the last commit
  live,        \* the key's value: its run and the commit that wrote it
  log,         \* the records written: seq, the durable mark, the value's run
  phys,        \* how many records are certainly on the drive
  vmem,        \* each run's value as written
  vdisk,       \* each run's values a crash may leave
  free,        \* runs free for a commit's value
  pend,        \* freed runs and the commit that freed them
  durable,     \* flusher.durable: the last commit known durable
  logged,      \* flusher.logged
  curLogged,   \* the published state's logged mark
  page,        \* the page holding the published tree
  pmem,        \* each page's tree as written
  pdisk,       \* each page's trees a crash may leave
  pfree,       \* pages free for a checkpoint
  ppend,       \* freed pages and the checkpoint that freed them
  ckpt,        \* the published checkpoint
  ckptWritten, \* flusher.ckptWritten
  ckptDurable, \* flusher.ckptDurable
  sbDur,       \* the superblock a crash keeps
  sbDurB,      \* the superblock a crash keeps once it keeps a later write
  physB,       \* phys, once a crash keeps a write after the last barrier
  vdiskB,      \* vdisk, likewise
  pdiskB,      \* pdisk, likewise
  sbNew,       \* the newest superblock written
  cpc,         \* saveTree's step: "idle", "barrier", "super", "publish"
  cnext        \* the checkpoint saveTree builds: its seq, page and tree
vars == <<platform, seq, live, log, phys, vmem, vdisk, free, pend, durable,
          logged, curLogged, page, pmem, pdisk, pfree, ppend, ckpt,
          ckptWritten, ckptDurable, sbDur, sbNew, cpc, cnext, sbDurB, physB,
          vdiskB, pdiskB>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Init ==
  /\ platform \in {"linux", "darwin"}
  /\ seq = 0
  /\ live = [slot |-> 1, tag |-> 0]
  /\ log = <<>>
  /\ phys = 0
  /\ vmem = [s \in Slots |-> IF s = 1 THEN 0 ELSE Empty]
  /\ vdisk = [s \in Slots |-> {vmem[s]}]
  /\ free = Slots \ {1}
  /\ pend = {}
  /\ durable = 0 /\ logged = 0 /\ curLogged = 0
  /\ page = "p1"
  /\ pmem = [p \in Pages |-> IF p = "p1" THEN [slot |-> 1, tag |-> 0] ELSE NoTree]
  /\ pdisk = [p \in Pages |-> {pmem[p]}]
  /\ pfree = {"p2"}
  /\ ppend = {}
  /\ ckpt = 0 /\ ckptWritten = 0 /\ ckptDurable = 0
  /\ sbDur = [seq |-> 0, page |-> "p1", tree |-> [slot |-> 1, tag |-> 0]]
  /\ sbNew = sbDur
  /\ sbDurB = sbDur /\ physB = 0 /\ vdiskB = vdisk /\ pdiskB = pdisk
  /\ cpc = "idle"
  /\ cnext = sbDur

\* A commit, under the writer lock: write the value into a free run, append
\* the record under the current durable mark, and free the replaced value.
Commit ==
  /\ cpc = "idle" /\ seq < MaxSeq
  /\ \E s \in free :
       /\ seq' = seq + 1
       /\ vmem' = [vmem EXCEPT ![s] = seq + 1]
       /\ vdisk' = [vdisk EXCEPT ![s] = @ \cup {seq + 1}]
       /\ vdiskB' = [vdiskB EXCEPT ![s] = @ \cup {seq + 1}]
       /\ free' = free \ {s}
       /\ pend' = pend \cup {[slot |-> live.slot, at |-> seq + 1]}
       /\ live' = [slot |-> s, tag |-> seq + 1]
       /\ log' = Append(log, [seq |-> seq + 1, dur |-> durable, slot |-> s])
       /\ curLogged' = Max({curLogged, durable})
  /\ UNCHANGED <<platform, phys, durable, logged, page, pmem, pdisk, pfree,
                 ppend, ckpt, ckptWritten, ckptDurable, sbDur, sbNew, cpc, cnext,
                 sbDurB, physB, pdiskB>>

\* releaseSpace, under the writer lock: return the values freed by commits
\* through the logged mark.
ReleaseValues ==
  /\ cpc = "idle"
  /\ \E e \in pend :
       /\ e.at <= logged
       /\ pend' = pend \ {e}
       /\ free' = free \cup {e.slot}
  /\ UNCHANGED <<platform, seq, live, log, phys, vmem, vdisk, durable, logged,
                 curLogged, page, pmem, pdisk, pfree, ppend, ckpt, ckptWritten,
                 ckptDurable, sbDur, sbNew, cpc, cnext, sbDurB, physB, vdiskB,
                 pdiskB>>

\* Every write so far is kept by a crash that keeps any later write.
Order ==
  /\ physB' = Len(log)
  /\ vdiskB' = [s \in Slots |-> {vmem[s]}]
  /\ pdiskB' = [p \in Pages |-> {pmem[p]}]
  /\ sbDurB' = sbNew

\* Every write so far reaches the drive.
Persist ==
  /\ Order
  /\ phys' = Len(log)
  /\ vdisk' = vdiskB'
  /\ pdisk' = pdiskB'
  /\ sbDur' = sbNew

\* syncThrough: flush, then raise the marks from the published state and the
\* newest superblock published.
Flush ==
  /\ Persist
  /\ durable' = seq
  /\ logged' = Max({logged, curLogged})
  /\ ckptDurable' = Max({ckptDurable, ckptWritten})
  /\ UNCHANGED <<platform, seq, live, log, vmem, free, pend, curLogged, page,
                 sbNew, pmem, pfree, ppend, ckpt, ckptWritten, cpc, cnext>>

\* saveTree writes the tree of the published commit into a free page and
\* frees the old tree's page at the new checkpoint.
CkptWrite ==
  /\ cpc = "idle" /\ ckpt < seq
  /\ \E q \in pfree :
       /\ pmem' = [pmem EXCEPT ![q] = live]
       /\ pdisk' = [pdisk EXCEPT ![q] = @ \cup {live}]
       /\ pdiskB' = [pdiskB EXCEPT ![q] = @ \cup {live}]
       /\ pfree' = pfree \ {q}
       /\ ppend' = ppend \cup {[page |-> page, at |-> seq]}
       /\ cnext' = [seq |-> seq, page |-> q, tree |-> live]
  /\ cpc' = "barrier"
  /\ UNCHANGED <<platform, seq, live, log, phys, vmem, vdisk, free, pend,
                 durable, logged, curLogged, page, ckpt, ckptWritten,
                 ckptDurable, sbDur, sbNew, sbDurB, physB, vdiskB>>

\* flushBarrier orders the tree before the superblock. A durable barrier
\* also raises the marks: setDurable(cur, prev).
Barrier ==
  /\ cpc = "barrier"
  /\ IF platform = "linux"
     THEN /\ Persist
          /\ durable' = seq
          /\ logged' = Max({logged, curLogged})
          /\ ckptDurable' = Max({ckptDurable, ckptWritten})
     ELSE /\ Order
          /\ UNCHANGED <<durable, logged, ckptDurable, phys, vdisk, pdisk, sbDur>>
  /\ cpc' = "super"
  /\ UNCHANGED <<platform, seq, live, log, vmem, free, pend, curLogged, page,
                 sbNew, pmem, pfree, ppend, ckpt, ckptWritten, cnext>>

SuperWrite ==
  /\ cpc = "super"
  /\ sbNew' = cnext
  /\ cpc' = "publish"
  /\ UNCHANGED <<platform, seq, live, log, phys, vmem, vdisk, free, pend,
                 durable, logged, curLogged, page, pmem, pdisk, pfree, ppend,
                 ckpt, ckptWritten, ckptDurable, sbDur, cnext, sbDurB, physB,
                 vdiskB, pdiskB>>

\* publish the state onto the new checkpoint, then flush.written.
Publish ==
  /\ cpc = "publish"
  /\ page' = cnext.page
  /\ ckpt' = cnext.seq
  /\ curLogged' = Max({curLogged, cnext.seq})
  /\ ckptWritten' = Max({ckptWritten, cnext.seq})
  /\ cpc' = "idle"
  /\ UNCHANGED <<platform, seq, live, log, phys, vmem, vdisk, free, pend,
                 durable, logged, pmem, pdisk, pfree, ppend, ckptDurable, sbDur,
                 sbNew, cnext, sbDurB, physB, vdiskB, pdiskB>>

\* releaseSpace and punch: return the pages freed by checkpoints through the
\* durable checkpoint.
ReleasePages ==
  /\ cpc = "idle"
  /\ \E e \in ppend :
       /\ e.at <= ckptDurable
       /\ ppend' = ppend \ {e}
       /\ pfree' = pfree \cup {e.page}
  /\ UNCHANGED <<platform, seq, live, log, phys, vmem, vdisk, free, pend,
                 durable, logged, curLogged, page, pmem, pdisk, ckpt,
                 ckptWritten, ckptDurable, sbDur, sbNew, cpc, cnext, sbDurB,
                 physB, vdiskB, pdiskB>>

Idle == UNCHANGED vars

Next ==
  \/ Commit \/ ReleaseValues \/ Flush \/ CkptWrite \/ Barrier \/ SuperWrite
  \/ Publish \/ ReleasePages \/ Idle
Spec == Init /\ [][Next]_vars

\* The choices a crash makes for each run or page among what it may hold.
Choices(d) ==
  {f \in [DOMAIN d -> UNION {d[x] : x \in DOMAIN d}] : \A x \in DOMAIN d : f[x] \in d[x]}

\* Recovery from superblock sb, the first n records, and the runs and pages
\* as vc and pc hold them: the tree is intact, the key reads back a value
\* that matches, and no commit known durable is lost.
Recovered(sb, n, vc, pc) ==
  LET idx == {i \in 1..n : log[i].seq > sb.seq}
      d == Max({log[i].dur : i \in idx} \cup {0})
      bad == {i \in idx : log[i].seq > d /\ vc[log[i].slot] # log[i].seq}
      kept == {i \in idx : \A j \in bad : i < j}
      last == IF kept = {} THEN [seq |-> sb.seq, slot |-> sb.tree.slot, tag |-> sb.tree.tag]
              ELSE [seq |-> log[Max(kept)].seq, slot |-> log[Max(kept)].slot,
                    tag |-> log[Max(kept)].seq]
  IN /\ pc[sb.page] = sb.tree
     /\ last.seq >= durable
     /\ vc[last.slot] = last.tag

\* A crash that keeps no write after the last barrier recovers from the
\* durable superblock; one that keeps a later write also keeps everything
\* before the barrier, and recovers from the superblock written before it or
\* the newest one.
RecoversCommitted ==
  /\ \A n \in phys..Len(log), vc \in Choices(vdisk), pc \in Choices(pdisk) :
       Recovered(sbDur, n, vc, pc)
  /\ \A sb \in {sbDurB, sbNew}, n \in physB..Len(log),
        vc \in Choices(vdiskB), pc \in Choices(pdiskB) :
       Recovered(sb, n, vc, pc)

\* The marks never pass what was flushed.
MarksFlushed == durable <= seq /\ logged <= durable /\ ckptDurable <= ckpt
====
