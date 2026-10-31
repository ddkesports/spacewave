---- MODULE SQLPrimary ----
\* A World whose block store writes to its SQL backend (sqlPrimaryStores), as
\* the single-writer deferred engine runs it. Each commit buffers its blocks in
\* the engine's BufferedStore; Sync drains them to SQL with INSERT IGNORE, runs
\* the SQL store's no-op Sync, then writes the durable head to s4db (commitFn).
\* In-process readers read through the buffer; peers and a restarted backend
\* read the durable head through the local bucket, read only, then SQL. The
\* uploader keeps copying the blocks its markers name from the bucket to SQL.
\*
\* Before the switch the head r0 and the block o it references sit in the
\* bucket with upload markers pending. Commit 1 writes the root n1, which
\* references o; commit 2 writes n2, which references n1 and o. A failed drain
\* write requeues its batch at the head of the queue, backs off and returns the
\* error, so Sync fails and a later drain retries the batch. A restart may remount with SQL primary off when Rollback holds: commits then
\* write to the bucket, and the account GC cleanup and eviction run again. The
\* GC sweeps bucket blocks its ref graph cannot reach from either head;
\* the graph records the references only of blocks written to the bucket.
\*
\* Not modeled: peers, the decoded cache, which holds only blocks already read,
\* a statement split across packets, which the buffer retries whole, the order
\* of the queue, which no durable head observes since Sync waits for it to
\* empty, and the reader pin poison, which loses no block.

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
  buf,      \* blocks the engine's BufferedStore holds
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

\* A commit writes its root: into the buffer under SQL primary, into the
\* bucket, with its marker and graph edges, otherwise. The buffer accepts it
\* while a failed drain waits to retry.
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

\* drainNextBatch: PutBlockBatch inserts the buffer and removes it. A failed
\* write requeues the batch, so the blocks stay buffered, and that drain's
\* Sync fails; the stutter after a failed Drain is its back-off.
Drain ==
  /\ buf # {} /\ dbUp
  /\ Store(buf) /\ buf' = {}
  /\ UNCHANGED <<mode, mem, dur, dbUp, local, marker, edges>>

\* syncHeadLocked: once the buffer drained, the SQL store's Sync returns at
\* once, and commitFn writes the head to s4db.
Sync ==
  /\ buf = {} /\ dur < mem
  /\ dur' = mem
  /\ UNCHANGED <<mode, mem, buf, dbUp, rows, acked, local, marker, edges>>

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

\* The backend crashes or restarts: it loses the buffer and the commits after
\* the durable head, and remounts, with SQL primary off when Rollback holds.
Restart ==
  /\ mem' = dur /\ buf' = {}
  /\ mode' \in IF Rollback THEN {"sql", "local"} ELSE {"sql"}
  /\ UNCHANGED <<dur, dbUp, rows, acked, local, marker, edges>>

Next ==
  \/ Commit \/ Drain \/ Sync \/ Upload \/ Evict \/ Sweep
  \/ DbCrash \/ DbRestart \/ Restart
  \/ UNCHANGED vars
Spec == Init /\ [][Next]_vars

\* What a store holds: the bucket, and MariaDB's rows.
Stored == local \cup rows \cup acked

\* Every block the durable head reaches stays stored, so a restarted backend
\* and every peer reads it.
DurableStored == Reach(dur) \subseteq Stored
\* A reader of the engine's published head finds each block it reaches in the
\* buffer or a store, so no reader sees a commit before its blocks.
PublishedReadable == Reach(mem) \subseteq buf \cup Stored

\* Liveness, under the fairness below: once MariaDB stays up, the durable
\* head keeps catching up with the published one, without a restart. Fairness
\* on Sync stands for a caller that retries a failed Sync.
FairSpec == Spec /\ WF_vars(Drain) /\ WF_vars(Sync) /\ WF_vars(DbRestart)
CatchesUp == <>[]dbUp => []<>(dur = mem)
====
