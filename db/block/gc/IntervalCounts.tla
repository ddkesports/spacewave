---- MODULE IntervalCounts ----
\* Shared counts across whole intervals: one World lineage commits into an
\* interval, names it, and the record its net produces applies by the push-down
\* rule, while puts, releases, pins and the replay hold change the same counts.
\* SharedCounts.tla checks the push-down rule on per-commit records over trees
\* that give each page one parent; this spec checks the code's netting instead:
\*
\* - ReplacementSteps.Join and ReplacementStep.Net (db/block/replacement-step.go)
\*   over every commit since the last name, with found pages from the put
\*   receipts as pendingNet.net folds them (core/sobject/world/engine);
\* - NetPuts, the record of an interval whose From the counts do not hold;
\* - the Volume's put counting, addExistedHolds (db/volume/common/kvtx), with
\*   MarkOriginal's counted entries in the naming batch;
\* - RefGraph.ResolveDeaths and ChangeHolds, in any walk order, and
\*   ResolveReleases (db/block/gc);
\* - the replay hold, a block marked HoldsRoots that names the last named root,
\*   as a holder that moves at each name and releases the root before;
\* - a reader pin, an owner edge that counts at once and releases later;
\* - a forfeit, an interval whose puts were dropped: it names no record, the
\*   save carries a recount, and the next interval is unfunded.
\*
\* Pages are content addressed, so Kids is fixed. L has two parents, A and B,
\* and R1 and R3 reach it through both: a write that copies a page under a
\* second parent, or moves it, is a commit between these roots. R4 names A
\* directly and through D, so a commit from R0 to R4 copies the stored A under
\* two new parents while the dead R0 named it.
\*
\* A commit replaces a parent-closed set of the current tree's pages, encodes
\* every page of the new tree the current one lacks, and writes back each
\* replaced page a written page still names; a replaced page only an untouched
\* page names may be written back or not. It puts some of its pages at once
\* when Overflow allows, as the page set's overflow does; the rest stay in
\* memory until the name puts the ones the new root reaches. Each put and each
\* name is one store transaction. A record applies as one transaction too: the
\* code reads the counts in one and writes their changes in another, and a put
\* between them can only change a free the deleter checks again
\* (FreeQueue.tla). A freed page leaves the store when its record or release
\* frees it. The naming save, the encoding of the record and the save that
\* names it are one step here; DeadlistLedger.tla checks their order. The
\* recount is atomic here; SharingWalk.tla checks its slices.
\*
\* Left out: failed publications, since the engine reinstalls the World from
\* before the failed commit, which leaves no step on the chain; a chain gap,
\* which nets from the puts as a forfeit's successor does; several lineages,
\* which SharedCounts.tla covers.
\*
\* IntervalCounts.cfg leaves overflow out, because the code's rules fail with
\* it. An overflow put counts a found page once, however many new parents
\* name it in memory, so Overflow "pages" fails CountsCover; DropRule "own"
\* closes a second such gap. Overflow puts of an old page or of the named root
\* also leak, failing CountsExact.
\*
\* Switches, each set to the code's rule in IntervalCounts.cfg; the mutant
\* configs flip one each:
\* - Overflow: "none", "whole" (a commit puts all its pages or none) or
\*   "pages" (any bottom-up subset).
\* - HeldRule: "parent" holds a page once per written parent naming it;
\*   "once" holds it once, skipping a page a dead original page named.
\* - FoundRule: "every" calls a page found when every put found it; "any"
\*   when any put did.
\* - Unfunded: "puts" nets an unfunded interval from its puts; "steps" nets
\*   it from its steps as a funded one.
\* - HoldRule: "always" counts the replay hold's edge to the named root;
\*   "found" counts it only when the root was already stored.
\* - Recount: a forfeit's save recounts.
\* - OriginalRule: "stored" pushes a page's children in the live walk as new
\*   edges only when the interval stored the page afresh: an original or found
\*   page is the stored page, whose edges counted, and which is held for
\*   each new edge to it, and listed existed without one, as an untouched
\*   page is; "created" pushes them for every created or replaced page,
\*   holds no written page, and lists a written page existed only when found.
\* - ListRule: "reached" lists a replaced original page dead only when the old
\*   root reaches it through dead original pages, since a page dirtied through
\*   a parent the step dropped may still sit under a parent no step touched;
\*   "all" lists every dead replaced page. "reached" also removes each dead
\*   page a stored replaced page names that is not listed replaced, written or
\*   not: a stored page's children are stored; "all" removes only pages no
\*   step wrote.
\* - MarkedParent: a marked entry the put finds stored stands in as the found
\*   parent of a found child, whose edge from it the store counted.
\* - FreshHeld: a page stored afresh is held once for each fresh parent
\*   naming it past its first holder; FALSE holds it never.
\* - WalkRule: "parents" decides each page of the death walk once all its
\*   walk parents are decided, taking one count per parent that frees;
\*   "first" decides it by whichever parent reaches it first.
\* - DropRule: "replaced", the code's rule, lists a dead page the interval
\*   stored afresh with the replaced pages, whose edges the walk takes back;
\*   "own" lists it as dropped and decides it alone, since no put counted its
\*   edges. Only an overflow put stores such a page, and "replaced" then
\*   takes counts that were never added.

EXTENDS Integers, FiniteSets, Sequences

CONSTANTS MaxCommits, Overflow, Pins, Forfeits,
          HeldRule, FoundRule, Unfunded, HoldRule, Recount, OriginalRule, ListRule,
          FreshHeld, WalkRule, DropRule,
          MarkedParent

None == "none"
Idle == [on |-> FALSE]  \* no record applying

Pages == {"L", "M", "A", "B", "D", "R0", "R1", "R2", "R3", "R4"}
Kids(p) ==
  CASE p \in {"A", "B"} -> {"L"}
    [] p = "D" -> {"A"}
    [] p = "R0" -> {"A"}
    [] p = "R1" -> {"A", "B"}
    [] p = "R2" -> {"B", "M"}
    [] p = "R3" -> {"D", "B"}
    [] p = "R4" -> {"D", "A"}
    [] OTHER -> {}
Roots == {"R0", "R1", "R2", "R3", "R4"}

RECURSIVE Reach(_)
Reach(p) == {p} \cup UNION {Reach(k) : k \in Kids(p)}

Sum(f, S) == LET RECURSIVE F(_) F(T) == IF T = {} THEN 0
                                         ELSE LET x == CHOOSE x \in T : TRUE
                                              IN f[x] + F(T \ {x})
             IN F(S)

\* The sets of the current tree's pages a commit may replace: the root and
\* any pages each with a parent in the set.
Closed(r) == {S \in SUBSET Reach(r) :
                r \in S /\ \A p \in S \ {r} : \E q \in S : p \in Kids(q)}

VARIABLES
  cur,       \* the root in memory
  named,     \* the root last named
  stored,    \* pages in the store
  extra,     \* each page's shared count: holders minus one
  rel,       \* each page's releases not yet resolved
  kept,      \* pages encoded this interval and held in memory
  steps,     \* the commits since the last name, oldest first
  putSet,    \* pages a put of this interval carried
  freshSet,  \* pages some put of this interval stored afresh
  foundSet,  \* pages some put of this interval found
  firstFresh,\* pages whose first put this interval stored afresh
  funded,    \* the engine counts on the lineage of named as a holder
  lineage,   \* ghost: the counts do hold the lineage of named
  queue,     \* named records not yet applied, oldest first
  walk,      \* the record applying, or Idle
  pin,       \* the reader pin's root, or None
  hold,      \* the replay hold's root
  commits,
  lossy,     \* a forfeit or unfunded record ran: leaks are allowed
  underflow  \* a count change went below zero
vars == <<cur, named, stored, extra, rel, kept, steps, putSet, freshSet,
          foundSet, firstFresh, funded, lineage, queue, walk, pin, hold,
          commits, lossy, underflow>>

PinRoots == IF pin = None THEN {} ELSE {pin}
Live == UNION {Reach(r) : r \in {named, hold} \cup PinRoots}
Holders(p) == Cardinality({q \in Live : p \in Kids(q)})
                + (IF p = hold THEN 1 ELSE 0)
                + (IF p = pin THEN 1 ELSE 0)
                + (IF p = named /\ lineage THEN 1 ELSE 0)

Init ==
  /\ cur = "R0" /\ named = "R0" /\ hold = "R0" /\ pin = None
  /\ stored = Reach("R0")
  /\ extra = [p \in Pages |-> IF p = "R0" THEN 1 ELSE 0]
  /\ rel = [p \in Pages |-> 0]
  /\ kept = {} /\ steps = <<>>
  /\ putSet = {} /\ freshSet = {} /\ foundSet = {} /\ firstFresh = {}
  /\ funded = TRUE /\ lineage = TRUE
  /\ queue = <<>> /\ walk = Idle
  /\ commits = 0 /\ lossy = FALSE /\ underflow = FALSE

\* ---------------------------------------------------------------------------
\* The Volume's put: addExistedHolds over a batch of entries E, of which the
\* writer marked K counted. existed is the store before the batch. H holds
\* the pages a hold block in the batch names, which never count as having no
\* parent there.

PutCounts(E, K, H) ==
  LET existed(p) == p \in stored
      keptK == {p \in E \cap K : existed(p)}
      revived == {p \in E \cap K : ~existed(p)}
      listed == (E \ K) \cup revived
      found(p) == p \in E \ K /\ existed(p)
      parents(k) == Cardinality({e \in listed \cup
                                   (IF MarkedParent THEN keptK ELSE {}) :
                                   k \in Kids(e)})
      fromNew(k) == IF k \in listed /\ found(k)
                    THEN Cardinality({e \in listed : ~found(e) /\ k \in Kids(e)})
                    ELSE 0
      noParent(k) == IF found(k) /\ parents(k) = 0 /\ k \notin H THEN 1 ELSE 0
      fromRevived(k) == Cardinality({e \in revived : k \in Kids(e)
                          /\ (k \in keptK \/ (k \notin listed /\ k \in stored))})
  IN [p \in Pages |-> (IF p \in E /\ ~existed(p) THEN 0 ELSE extra[p])
                       + fromNew(p) + noParent(p) + fromRevived(p)]

\* The receipts of a batch's unmarked entries, folded into the interval's.
Receipts(E, K) ==
  LET R == E \ K
      fresh == R \ stored
  IN /\ putSet' = putSet \cup R
     /\ freshSet' = freshSet \cup fresh
     /\ foundSet' = foundSet \cup (R \cap stored)
     /\ firstFresh' = firstFresh \cup (fresh \ putSet)

\* ---------------------------------------------------------------------------
\* Commit moves the World in memory from cur to r. It replaces Rp, encodes C,
\* and puts O of C at once.

OverflowSets(C) ==
  CASE Overflow = "none" -> {{}}
    [] Overflow = "whole" -> {{}, C}
    [] OTHER -> {O \in SUBSET C : \A p \in O : Kids(p) \subseteq O \cup stored}

Commit ==
  /\ ~walk.on
  /\ commits < MaxCommits
  /\ \E r \in Roots \ {cur}, Rp \in Closed(cur) :
     \E W \in SUBSET (Rp \cap Reach(r)) :
     LET C == (Reach(r) \ Reach(cur)) \cup W \cup {r} IN
     \* A dirty page a written page still names is encoded again.
     /\ \A p \in C : Kids(p) \cap Rp \subseteq C
     /\ \E O \in OverflowSets(C) :
        /\ steps' = Append(steps, [from |-> cur, to |-> r,
                                   replaced |-> Rp, created |-> C])
        /\ kept' = (kept \ O) \cup (C \ O)
        /\ extra' = PutCounts(O, {}, {})
        /\ stored' = stored \cup O
        /\ Receipts(O, {})
        /\ cur' = r
  /\ commits' = commits + 1
  /\ UNCHANGED <<named, rel, funded, lineage, queue, walk, pin, hold, lossy, underflow>>

\* ---------------------------------------------------------------------------
\* Join and Net, as the code computes them for interval s with found pages F
\* and carried pages P.

Original(s) == UNION {{p \in s[i].replaced :
                        \A j \in 1..(i - 1) : p \notin s[j].created}
                      : i \in 1..Len(s)}

\* MarkOriginal's counted entries: a page some step replaced before any
\* created it, other than the first root.
IsOriginal(s, p) ==
  \E i \in 1..Len(s) : p \in s[i].replaced
                       /\ \A j \in 1..(i - 1) : p \notin s[j].created

Net(s, F, P) ==
  LET from == s[1].from
      to == s[Len(s)].to
      replaced == UNION {s[i].replaced : i \in 1..Len(s)}
      created == UNION {s[i].created : i \in 1..Len(s)}
      original == Original(s)
      isStored(p) == p \in original \/ p \in P
      foundPages == {p \in created : p \in F /\ p \notin original}
      createdNet == created \ foundPages
      RECURSIVE NetReach(_)
      NetReach(p) == {p} \cup IF p \in created \cup replaced
                              THEN UNION {NetReach(k) : k \in Kids(p)} ELSE {}
      live == NetReach(to)
      \* An original page's put was counted, so under "stored" it stands with
      \* the found pages: its stored copy's edges were counted already.
      directNet == IF OriginalRule = "stored" THEN createdNet \ original
                   ELSE createdNet
      directSrc == IF OriginalRule = "stored"
                   THEN live \cap (createdNet \ original)
                   ELSE live \cap (createdNet \cup replaced)
      direct(k) == (IF k = to THEN 1 ELSE 0)
                     + Cardinality({p \in directSrc : k \in Kids(p)})
      RECURSIVE DeadOriginal(_)
      DeadOriginal(p) == {p} \cup UNION {DeadOriginal(k) :
                           k \in {c \in Kids(p) : c \in replaced \cap original
                                                  /\ c \notin live}}
      deadOriginal == IF from \in live THEN {} ELSE DeadOriginal(from)
      untouched == IF ListRule = "all" THEN {}
                   ELSE ((replaced \cap original) \ live) \ deadOriginal
      replacedStored == {p \in replaced \ untouched : isStored(p)}
      replacedList == (replacedStored \ live)
                        \cup {p \in created : p \notin live /\ p \notin replaced
                                              /\ isStored(p)}
      \* A dead page stored afresh named its children by edges no put counted.
      dropped == IF DropRule = "own"
                 THEN replacedList \cap (createdNet \ original) ELSE {}
      namedKids == UNION {Kids(p) : p \in replacedStored \ dropped}
      readNamed == UNION {Kids(p) : p \in (replacedStored \ live) \cap original}
      removed == {x \in namedKids \cup {from} :
                    x \notin live /\
                    IF ListRule = "all"
                    THEN x \notin replaced /\ x \notin created
                    ELSE x \notin replacedList /\ x \notin untouched}
      \* A found page's holders were counted at its put, and a page stored
      \* afresh takes its first holder without a count.
      heldBase(k) == k \in live /\ k # from /\ k # to
                     /\ IF OriginalRule = "stored"
                        THEN k \notin created \ original \/ k \in directNet
                        ELSE k \notin replaced /\ k \notin created
      edges(k) == IF OriginalRule = "stored" /\ k \in directNet
                  THEN IF FreshHeld /\ direct(k) > 0 THEN direct(k) - 1 ELSE 0
                  ELSE direct(k)
      held == [k \in Pages |->
                 IF ~heldBase(k) THEN 0
                 ELSE IF HeldRule = "parent" THEN edges(k)
                 ELSE IF edges(k) > 0 /\ k \notin readNamed THEN 1 ELSE 0]
  IN [from |-> from, replaced |-> replacedList \ dropped, dropped |-> dropped,
      removed |-> removed,
      kept |-> live \ readNamed, held |-> held,
      existed |-> {k \in live : k \in foundPages
                                \/ (direct(k) = 0 /\ k \notin directNet)}]

\* NetPuts: hold each child once per edge from a page stored afresh, less a
\* fresh child's first holder.
NetPuts(P, fresh) ==
  LET edges(k) == Cardinality({e \in P \cap fresh : k \in Kids(e)})
  IN [k \in Pages |-> IF edges(k) = 0 THEN 0
                      ELSE edges(k) - (IF k \in P /\ k \in fresh THEN 1 ELSE 0)]

\* ---------------------------------------------------------------------------
\* Name puts the pages in memory the root reaches, moves the replay hold to
\* the root, and names the interval's record. Forfeit does the same save with
\* the interval's puts dropped: no record, and a recount.

RECURSIVE KeptReach(_)
KeptReach(p) == IF p \in kept THEN {p} \cup UNION {KeptReach(k) : k \in Kids(p)}
                ELSE {}

\* The save: the naming batch's counts, with the replay hold block naming the
\* root. The batch always carries the root, reread from the store when it is
\* not in memory. A hold block naming the same root as the last one is the
\* same block, found with its child, so the root gains nothing; a new one
\* counts its edge to the root, fresh or found.
SaveCounts(E, K) ==
  LET put == PutCounts(E, K, {cur})
      holdAdd == IF hold = cur THEN 0
                 ELSE IF HoldRule = "always" \/ cur \in stored THEN 1 ELSE 0
  IN [p \in Pages |-> put[p] + (IF p = cur THEN holdAdd ELSE 0)]

MoveHold == /\ hold' = cur
            /\ rel' = IF hold = cur THEN rel ELSE [rel EXCEPT ![hold] = @ + 1]

Name ==
  /\ ~walk.on
  /\ steps # <<>>
  /\ LET E == KeptReach(cur) \cup {cur}
         K == {p \in E : IsOriginal(steps, p) /\ p # steps[1].from}
         saved == SaveCounts(E, K)
         P == putSet \cup (E \ K)
         fresh == freshSet \cup ((E \ K) \ stored)
         foundAll == P \ fresh
         foundAny == foundSet \cup ((E \ K) \cap stored)
         F == IF FoundRule = "every" THEN foundAll ELSE foundAny
         netted == funded \/ Unfunded = "steps"
         rec == Net(steps, F, P)
         held == IF netted THEN rec.held ELSE NetPuts(P, fresh)
     IN /\ stored' = stored \cup E
        /\ extra' = [p \in Pages |-> saved[p] + held[p]]
        /\ queue' = IF netted THEN Append(queue, rec) ELSE queue
        \* The root's first put this interval takes the lineage as its first
        \* holder when it stores the root afresh; a found root gains only the
        \* hold block's edge. A World back at the named root keeps its lineage.
        /\ funded' = (cur \in firstFresh \cup ((E \ K) \ (stored \cup putSet)))
        /\ lineage' = (funded' \/ (cur = named /\ lineage))
        /\ lossy' = (lossy \/ ~funded)
  /\ MoveHold
  /\ named' = cur
  /\ steps' = <<>> /\ kept' = {}
  /\ putSet' = {} /\ freshSet' = {} /\ foundSet' = {} /\ firstFresh' = {}
  /\ UNCHANGED <<cur, walk, pin, commits, underflow>>

\* The recount counts each stored page's holders from the graph: the stored
\* pages naming it, the pin, the replay hold, and the From of each named
\* record. The lineage of a root no record names yet is not among them.
RecountOf(st, h) ==
  LET holders(p) == Cardinality({q \in st : p \in Kids(q)})
                      + (IF p = h THEN 1 ELSE 0)
                      + (IF p = pin THEN 1 ELSE 0)
                      + Cardinality({i \in 1..Len(queue) : queue[i].from = p})
  IN [p \in Pages |-> IF p \in st /\ holders(p) > 0 THEN holders(p) - 1 ELSE 0]

Forfeit ==
  /\ Forfeits
  /\ ~walk.on
  /\ steps # <<>>
  /\ LET E == KeptReach(cur) \cup {cur}
         K == {p \in E : IsOriginal(steps, p) /\ p # steps[1].from}
     IN /\ stored' = stored \cup E
        \* The recount's walk sees the roots the save applies; the edge the
        \* save removes falls while the counts are incomplete, which drops it.
        /\ IF Recount
           THEN /\ extra' = RecountOf(stored \cup E, cur)
                /\ hold' = cur
                /\ rel' = rel
           ELSE /\ extra' = SaveCounts(E, K)
                /\ MoveHold
  /\ funded' = FALSE
  /\ lineage' = ~Recount
  /\ lossy' = TRUE
  /\ named' = cur
  /\ steps' = <<>> /\ kept' = {}
  /\ putSet' = {} /\ freshSet' = {} /\ foundSet' = {} /\ firstFresh' = {}
  /\ UNCHANGED <<cur, queue, walk, pin, commits, underflow>>

\* ---------------------------------------------------------------------------
\* Apply runs ResolveDeaths on the oldest record, deciding each page of the
\* walk once all its parents in the walk are decided, in any such order, then
\* ChangeHolds. Nothing else runs meanwhile.

\* A dropped page's edges were never counted, so the walk does not follow them.
Children(n, dropped) == IF n \in stored /\ n \notin dropped THEN Kids(n) ELSE {}

\* The walk: the listed pages no listed page names, and every page below them
\* through children that are not live.
WalkFrom(roots, live, dropped) ==
  LET RECURSIVE Down(_)
      Down(n) == {n} \cup UNION {Down(c) : c \in Children(n, dropped) \ live}
  IN UNION {Down(r) : r \in roots}

StartApply ==
  /\ ~walk.on
  /\ queue # <<>>
  /\ LET rec == Head(queue)
         listed == rec.replaced \cup rec.removed \cup rec.dropped
         namedL == {c \in listed : \E n \in listed : c \in Children(n, rec.dropped)}
         live == rec.kept \cup {c \in UNION {Children(n, {}) : n \in rec.replaced} :
                                  c \notin listed}
     IN walk' = [on |-> TRUE, rec |-> rec, listed |-> listed, live |-> live,
                 dropped |-> rec.dropped,
                 counted |-> {k \in Pages : rec.held[k] > 0} \cup rec.existed,
                 roots |-> listed \ namedL,
                 nodes |-> WalkFrom(listed \ namedL, live, rec.dropped),
                 done |-> {}, frees |-> {},
                 changes |-> [p \in Pages |-> 0]]
  /\ queue' = Tail(queue)
  /\ UNCHANGED <<cur, named, stored, extra, rel, kept, steps, putSet, freshSet,
                 foundSet, firstFresh, funded, lineage, pin, hold, commits, lossy,
                 underflow>>

\* A page loses one count for each parent in the walk that frees, and a root
\* of the walk one more; it lives while its count covers them.
WalkStep ==
  /\ walk.on
  /\ \E n \in walk.nodes \ walk.done :
     LET parents == {q \in walk.nodes : n \in Children(q, walk.dropped)}
         Decide(killed) ==
           LET counts == killed > 0 /\ extra[n] + walk.changes[n] >= killed
               lives == killed = 0 \/ counts
               kids == Children(n, walk.dropped)
               delta(c) == IF c \in walk.live
                           THEN IF lives /\ c \notin walk.counted THEN 1
                                ELSE IF ~lives /\ c \in walk.counted THEN -1
                                ELSE 0
                           ELSE 0
           IN walk' = [walk EXCEPT
                         !.done = @ \cup {n},
                         !.frees = IF lives THEN @ ELSE @ \cup {n},
                         !.changes = [p \in Pages |-> @[p]
                                        - (IF p = n /\ counts THEN killed ELSE 0)
                                        + (IF p \in kids THEN delta(p) ELSE 0)]]
     IN IF WalkRule = "parents"
        THEN /\ parents \subseteq walk.done
             /\ Decide((IF n \in walk.roots THEN 1 ELSE 0)
                         + Cardinality(parents \cap walk.frees))
        ELSE \E q \in (parents \cap walk.done)
                   \cup (IF n \in walk.roots THEN {"root"} ELSE {}) :
               Decide(IF q = "root" \/ q \in walk.frees THEN 1 ELSE 0)
  /\ UNCHANGED <<cur, named, stored, extra, rel, kept, steps, putSet, freshSet,
                 foundSet, firstFresh, funded, lineage, queue, pin, hold, commits, lossy,
                 underflow>>

EndApply ==
  /\ walk.on
  /\ walk.done = walk.nodes
  /\ LET rec == walk.rec
         rootLoss == IF rec.from \notin walk.listed /\ rec.from \in rec.existed
                     THEN 1 ELSE 0
         changes == [p \in Pages |-> walk.changes[p]
                                     - (IF p = rec.from THEN rootLoss ELSE 0)]
         next(p) == extra[p] + changes[p]
     IN /\ underflow' = (underflow \/ \E p \in Pages : next(p) < 0)
        /\ extra' = [p \in Pages |-> IF next(p) < 0 THEN 0 ELSE next(p)]
        /\ stored' = stored \ walk.frees
  /\ walk' = Idle
  /\ UNCHANGED <<cur, named, rel, kept, steps, putSet, freshSet, foundSet,
                 firstFresh, funded, lineage, queue, pin, hold, commits, lossy>>

\* ---------------------------------------------------------------------------
\* Releases, the reader pin.

Resolve(p) ==
  /\ ~walk.on
  /\ rel[p] > 0
  /\ IF extra[p] > 0
     THEN /\ extra' = [extra EXCEPT ![p] = @ - 1]
          /\ rel' = [rel EXCEPT ![p] = @ - 1]
          /\ UNCHANGED stored
     ELSE /\ stored' = stored \ {p}
          /\ rel' = [q \in Pages |-> rel[q] - (IF q = p THEN 1 ELSE 0)
                                    + (IF q \in Children(p, {}) THEN 1 ELSE 0)]
          /\ UNCHANGED extra
  /\ UNCHANGED <<cur, named, kept, steps, putSet, freshSet, foundSet,
                 firstFresh, funded, lineage, queue, walk, pin, hold, commits, lossy,
                 underflow>>

Pin ==
  /\ Pins
  /\ ~walk.on
  /\ pin = None
  /\ commits < MaxCommits
  /\ pin' = named
  /\ extra' = [extra EXCEPT ![named] = @ + 1]
  /\ commits' = commits + 1
  /\ UNCHANGED <<cur, named, stored, rel, kept, steps, putSet, freshSet,
                 foundSet, firstFresh, funded, lineage, queue, walk, hold, lossy,
                 underflow>>

Unpin ==
  /\ ~walk.on
  /\ pin # None
  /\ pin' = None
  /\ rel' = [rel EXCEPT ![pin] = @ + 1]
  /\ UNCHANGED <<cur, named, stored, extra, kept, steps, putSet, freshSet,
                 foundSet, firstFresh, funded, lineage, queue, walk, hold, commits,
                 lossy, underflow>>

Next ==
  \/ Commit \/ Name \/ Forfeit
  \/ StartApply \/ WalkStep \/ EndApply
  \/ \E p \in Pages : Resolve(p)
  \/ Pin \/ Unpin
  \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars

Quiet == steps = <<>> /\ queue = <<>> /\ ~walk.on
         /\ \A p \in Pages : rel[p] = 0

\* The tree in memory reads: each page is in memory or stored.
MemoryReads == \A p \in Reach(cur) : p \in kept \/ p \in stored

\* No named, held or pinned root reaches a freed page.
NoFreedRead == Live \subseteq stored

\* A release names a stored page, so no page frees twice.
ReleasesStored == \A p \in Pages : rel[p] > 0 => p \in stored

\* The engine counts on the lineage only where the counts hold it.
FundedHeld == funded => lineage

\* No count change went below zero. An unfunded interval frees pages through
\* their holds alone, so an earlier record's live set can name a page that
\* died since; the applier clamps that release at zero, which leaks the page.
NoUnderflow == ~lossy => ~underflow

\* Once everything applied, no count is below the page's holders minus one.
CountsCover == Quiet => \A p \in Live : extra[p] >= Holders(p) - 1

\* Without a forfeit or unfunded record, each count is exact once everything
\* applied, and the store holds only live pages.
CountsExact == (Quiet /\ ~lossy) => \A p \in Live : extra[p] = Holders(p) - 1
NoLeak == (Quiet /\ ~lossy) => stored = Live
====
