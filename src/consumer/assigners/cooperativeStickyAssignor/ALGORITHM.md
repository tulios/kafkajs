# CooperativeStickyAssignor — algorithm

This is the deep-dive companion to the user-facing
[`docs/Consuming.md` cooperative-sticky section](../../../../docs/Consuming.md#cooperative-sticky-assigner).
It documents the *stable shape* of the algorithm, not line-level details. The
vocabulary mirrors Apache Kafka's `CooperativeStickyAssignor` /
[KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)
so a reader familiar with the Java assignor can map concepts directly.

## Two responsibilities

1. **Sticky assignment** (leader-side, pure): given every member's owned
   partitions, produce a balanced assignment (max 1-partition skew within a
   subscription group) that preserves as many already-owned partitions as
   possible.
2. **Cooperative rebalancing** (KIP-429): never hand a partition to its new
   owner while the previous owner still holds it. Partitions that must move are
   withheld for one round, the old owner revokes them, and the freed partition
   is picked up in a follow-up round.

## Sticky core (leader)

Runs only on the group leader, in `assign()`. It is a pure function of the
decoded member subscriptions plus current cluster metadata
(`stickyAssign.js`):

1. **Seed from owned** — each member tentatively retains the partitions it
   already owns, as long as the partition still exists in metadata and is part
   of a subscribed topic. Conflicting claims (two members owning the same
   partition across a rebalance gap) are resolved by **generation**: the claim
   tagged with the higher `generationId` wins; ties broken deterministically by
   memberId.
2. **Free-fill** — partitions nobody validly retained are greedily handed to the
   least-loaded subscribed member.
3. **Bounded rebalance** — a final pass evens out load to the 1-partition-skew
   invariant, preferring to move *non-owned* partitions so stickiness is
   disturbed as little as possible.

The result is the *ideal eager assignment*: where every partition should end up
once the group is stable.

## Cooperative adjustment (leader)

The ideal assignment can hand a partition to a new owner while the old owner
still holds it. Committing to that directly would double-own the partition
across the rebalance gap. So we compute, in `cooperative.js`:

```
transferring = { p | p is in ideal[newOwner] AND p is still owned by some other member }
adjusted[m]  = ideal[m] \ transferring          // strip transferring from this round
```

Each `transferring` partition is **withheld** from its new owner this round.
The old owner sees the partition missing from its own assignment and revokes it
on SYNC. The freed partition is then assigned in the next round.

## Two-round withhold (member, runner-driven)

Each member runs JOIN → SYNC. The cooperative SYNC branch
(`consumerGroup.js#applyCooperativeAssignment`) diffs what it owned against the
new assignment:

```
revoked = owned − newAssigned       added = newAssigned − owned
```

Revoked-partition offsets are committed *before* the partitions are dropped
(under the new generation — JOIN+SYNC already advanced it), retained partitions
keep their resolved-but-uncommitted offsets, and `revoked.length > 0` arms a
second JOIN round so the withheld partitions get assigned.

```
 Round 1                              Round 2
 ───────                              ───────
 C1 owns p0,p1,p2,p3                  C1: p0,p1,p2,p3 → p0,p1   (revoke p2,p3 committed)
 C2 joins (owns nothing)              C2: {} → p2,p3            (p2,p3 now free, assigned)
 leader withholds p2,p3 from C2
 C1 revokes p2,p3
        │                                    │
        └──── rejoinNeeded = true ───────────┘
              (revoke happened)              converged: nothing revoked → stop
```

Normal KIP-429 churn converges in **2 rounds**. The round counter
(`cooperativeRounds`) is capped by `maxCooperativeRejoinRounds` (default `2`);
beyond the cap the **degraded path** commits current offsets, warns loudly, and
stops looping. The withheld partitions stay unowned until the next natural
rebalance re-assigns them — a bounded, recoverable gap, preferred over an
unbounded rejoin loop. Raising the cap is a defensive escape hatch, not a tuning
knob.

## Conflict resolution by generation

When two members both advertise ownership of the same partition (e.g. a member
rejoined under a stale generation), the partition is awarded to the claim with
the higher `generationId`. `generationId` and `ownedPartitions` are threaded
into each member's JoinGroup metadata via the assigner `userData` buffer
(`userData.js`), because KafkaJS has no native `ownedPartitions` wire field.

## KafkaJS-only interop

Because owned partitions + generation ride in `userData` rather than Java's
native `ConsumerProtocolSubscription.ownedPartitions` field, this assigner
interoperates only within groups where every member is a KafkaJS consumer using
`cooperativeSticky` — not in mixed Java/KafkaJS groups. See the migration note
in `docs/Consuming.md`.
