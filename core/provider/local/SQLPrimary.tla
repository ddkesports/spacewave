---- MODULE SQLPrimary ----
\* A World whose block store writes to its SQL backend (sqlPrimaryStores), as
\* the single-writer deferred engine runs it with the SQL publisher
\* (bstore-sql-publish.go). Each commit is admitted under the writer lock:
\* its blocks join the publisher's overlay and queue for insertion, and the
\* engine's published head advances at once, before MariaDB holds a row.
\* Workers insert the queued blocks with INSERT IGNORE, several at a time and
\* in any order. The committer writes the durable head of commit k to s4db
\* (the local publisher) only after commit k's rows landed and commit k-1's
\* head was written, so the durable head advances one commit at a time. A
\* head that fails to commit fails every commit admitted after it, and the
\* engine reinstalls from the durable head; their queued blocks still land.
\* In-process readers read through the overlay; peers and a restarted backend
\* read the durable head through the local bucket, read only, then SQL. The
\* uploader keeps copying the blocks its markers name from the bucket to SQL.
\*
\* Before the switch the head r0 and the block o it references sit in the
\* bucket with upload markers pending. Commit 1 writes the root n1, which
\* references o; commit 2 writes n2, which references n1 and o. A failed
\* insert stays queued and its worker retries it after a back-off. A restart
\* drops the overlay and the commits after the durable head, and may remount
\* with SQL primary off when Rollback holds: commits then write to the
\* bucket, and the account GC cleanup and eviction run again. The GC sweeps
\* bucket blocks its ref graph cannot reach from either head; the graph
\* records the references only of blocks written to the bucket.
\*
\* Not modeled: peers, the decoded cache, which holds only blocks already read,
\* the chunking of a commit's blocks, whose rows the committer waits for as a
\* whole, a block shared with an earlier commit, which that commit inserts
\* and commits first, and the admission bound, which only delays Commit.

EXTENDS Naturals

\* AckDurable: MariaDB has made a row durable when its INSERT returns, as
\* innodb_flush_log_at_trx_commit = 1 does.
AckDurable == TRUE
\* Rollback: a restart may turn SQL primary off.
Rollback == FALSE

MaxCommit == 2
Root == <<"r0", "n1", "n2">>
RootOf(k) == Root[k + 1]
Refs == [b \in {"r0", "o", "n1", "n2"} |->
          CASE b = "r0" -> {"o"}
            [] b = "n1" -> {"o"}
            [] b = "n2" -> {"n1", "o"}
            [] OTHER -> {}]
Reach(k) == {RootOf(k)} \cup Refs[RootOf(k)]
            \cup UNION {Refs[c] : c \in Refs[RootOf(k)]}

VARIABLES
  mode,     \* "sql" while SQL primary, "local" after a rollback
  mem,      \* the engine's published head
  dur,      \* the durable head in s4db
  buf,      \* admitted blocks the publisher's overlay holds, not landed
  dbUp,     \* MariaDB answers
  rows,     \* rows MariaDB holds durably
  acked,    \* rows MariaDB acknowledged but has not made durable
  local,    \* blocks in the local bucket
  marker,   \* blocks with an upload marker pending
  edges     \* references the bucket's ref graph records
vars == <<mode, mem, dur, buf, dbUp, rows, acked, local, marker, edges>>

Init ==
  /\ mode = "sql" /\ mem = 0 /\ dur = 0 /\ buf = {}
  /\ dbUp = TRUE /\ rows = {} /\ acked = {}
  /\ local = {"r0", "o"} /\ marker = {"r0", "o"}
  /\ edges = {<<"r0", "o">>}

\* MariaDB stores an acknowledged row durably, or only in memory.
Store(S) ==
  IF AckDurable
  THEN rows' = rows \cup S /\ UNCHANGED acked
  ELSE acked' = acked \cup (S \ rows) /\ UNCHANGED rows

\* A commit writes its root: into the overlay under SQL primary, into the
\* bucket, with its marker and graph edges, otherwise. The published head
\* advances before any row lands. Admission continues while an insert waits
\* to retry.
Commit ==
  /\ mem < MaxCommit
  /\ LET b == RootOf(mem + 1) IN
     IF mode = "sql"
     THEN buf' = buf \cup {b} /\ UNCHANGED <<local, marker, edges>>
     ELSE /\ local' = local \cup {b} /\ marker' = marker \cup {b}
          /\ edges' = edges \cup {<<b, c>> : c \in Refs[b]}
          /\ UNCHANGED buf
  /\ mem' = mem + 1
  /\ UNCHANGED <<mode, dur, dbUp, rows, acked>>

\* runWorker: one insert lands any queued block, in any order, and drops it
\* from the overlay. A failed insert keeps it queued; the stutter is its
\* back-off.
Drain ==
  /\ dbUp
  /\ \E b \in buf : Store({b}) /\ buf' = buf \ {b}
  /\ UNCHANGED <<mode, mem, dur, dbUp, local, marker, edges>>

\* runCommitter: once the next commit's rows landed, its head commits to
\* s4db after the one before it. A local-store commit writes its head at
\* once, its blocks already in the bucket.
Sync ==
  /\ dur < mem
  /\ RootOf(dur + 1) \notin buf
  /\ dur' = dur + 1
  /\ UNCHANGED <<mode, mem, buf, dbUp, rows, acked, local, marker, edges>>

\* The next head fails to commit: it and every commit admitted after it
\* fail, and the engine reinstalls the durable head. Their queued blocks
\* still land, as rows no head reaches.
SyncFail ==
  /\ dur < mem
  /\ mem' = dur
  /\ UNCHANGED <<mode, dur, buf, dbUp, rows, acked, local, marker, edges>>

\* The uploader copies a marked block from the bucket to SQL.
Upload ==
  /\ dbUp
  /\ \E b \in marker \cap local :
       /\ Store({b})
       /\ marker' = marker \ {b}
  /\ UNCHANGED <<mode, mem, dur, buf, dbUp, local, edges>>

\* Eviction, off under SQL primary, drops an uploaded block from the bucket.
Evict ==
  /\ mode = "local"
  /\ \E b \in local \ marker :
       /\ b \in rows \cup acked
       /\ local' = local \ {b}
  /\ UNCHANGED <<mode, mem, dur, buf, dbUp, rows, acked, marker, edges>>

\* The account GC cleanup, off under SQL primary, sweeps bucket blocks the
\* graph cannot reach from the durable head or the engine's pinned head, with
\* their markers.
GraphReach ==
  LET roots == {RootOf(dur), RootOf(mem)}
      one == {c \in local : \E r \in roots : <<r, c>> \in edges}
  IN roots \cup one \cup {c \in local : \E p \in one : <<p, c>> \in edges}
Sweep ==
  /\ mode = "local"
  /\ local \ GraphReach # {}
  /\ local' = local \cap GraphReach
  /\ marker' = marker \cap GraphReach
  /\ UNCHANGED <<mode, mem, dur, buf, dbUp, rows, acked, edges>>

\* MariaDB crashes, losing the rows it had not made durable, and restarts.
DbCrash ==
  /\ dbUp' = FALSE /\ acked' = {}
  /\ UNCHANGED <<mode, mem, dur, buf, rows, local, marker, edges>>
DbRestart ==
  /\ ~dbUp /\ dbUp' = TRUE
  /\ UNCHANGED <<mode, mem, dur, buf, rows, acked, local, marker, edges>>

\* The backend crashes or restarts: it loses the overlay and the commits
\* after the durable head, whose rows may have landed already, and remounts,
\* with SQL primary off when Rollback holds.
Restart ==
  /\ mem' = dur /\ buf' = {}
  /\ mode' \in IF Rollback THEN {"sql", "local"} ELSE {"sql"}
  /\ UNCHANGED <<dur, dbUp, rows, acked, local, marker, edges>>

Next ==
  \/ Commit \/ Drain \/ Sync \/ SyncFail \/ Upload \/ Evict \/ Sweep
  \/ DbCrash \/ DbRestart \/ Restart
  \/ UNCHANGED vars
Spec == Init /\ [][Next]_vars

\* What a store holds: the bucket, and MariaDB's rows.
Stored == local \cup rows \cup acked

\* Every block the durable head reaches stays stored, so a restarted backend
\* and every peer reads it.
DurableStored == Reach(dur) \subseteq Stored
\* The durable head reaches only rows MariaDB holds: the root of every
\* durable commit has landed.
DurableRows == \A k \in 1..dur : RootOf(k) \in rows \cup acked
\* A reader of the engine's published head finds each block it reaches in the
\* overlay or a store, so no reader sees a commit before its blocks.
PublishedReadable == Reach(mem) \subseteq buf \cup Stored

\* Liveness, under the fairness below: once MariaDB stays up, the durable
\* head keeps catching up with the published one, without a restart. Fairness
\* on Drain stands for workers that retry a failed insert.
FairSpec == Spec /\ WF_vars(Drain) /\ WF_vars(Sync) /\ WF_vars(DbRestart)
CatchesUp == <>[]dbUp => []<>(dur = mem)
====
