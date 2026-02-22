# Cooperative Sticky Assigner – Algorithm

This document describes the cooperative sticky partition assignment algorithm
implemented in `index.js`.  The design mirrors Kafka's Java
`CooperativeStickyAssignor` (KIP-429) and its underlying
`AbstractStickyAssignor` / `ConstrainedAssignmentBuilder`.

---

## Goals

| Property    | Description |
|-------------|-------------|
| **Sticky**  | Partitions stay with their current owner whenever possible, minimising movement during rebalances. |
| **Cooperative** | Only partitions whose ownership *changes* are briefly unassigned.  All other partitions remain assigned, so consumers can continue processing without interruption. |
| **Balanced** | The final distribution differs by at most 1 partition between any two members. |

---

## Data flow

```
Consumer instance                           Group leader (runs assign())
─────────────────                           ─────────────────────────────
protocol()  ─ encode ownedPartitions ──►    Step 1: decode all members
              + generationId into                   metadata
              userData                      Step 2: resolve conflicts
                                            Step 3: sticky assignment
                                            Step 4: quota-aware balancing
                                            Step 5: cooperative adjustment
                                            Step 6: encode + return
         ◄── memberAssignment ──────────

onAssignment()  ─ detect revocations ──►    (triggers round 2 if needed)
```

---

## Algorithm steps

### Step 1 – Parse member metadata

Each consumer encodes two fields into `userData` during `protocol()`:

- **generationId** – the generation of the last successful sync.
- **ownedPartitions** – `{ topic: [partitionId, ...] }` map of currently-held
  partitions.

The leader decodes every member's metadata to reconstruct the previous
ownership map.

### Step 2 – Resolve ownership conflicts

Two members may both claim the same partition (e.g. after a rolling restart).
Resolution rules:

| Condition | Outcome |
|-----------|---------|
| Same generation, same partition | **Contested** – treated as unowned. |
| Different generations | Higher generation wins ownership. |

### Step 3 – Sticky assignment

For each topic-partition, if the validated previous owner is still a group
member and is subscribed to the topic, assign the partition back to that member.
All other partitions (new, orphaned, or contested) go into the **unassigned pool**.

### Step 4 – Quota-aware balancing

This is the core balancing step, modelled after Java's
`ConstrainedAssignmentBuilder`.

```
P = total number of partitions across all topics
M = number of group members

minQuota = floor(P / M)
maxQuota = ceil(P / M)
expectedMaxQuotaMembers = P % M     // only this many may keep maxQuota
```

**Phase 1 – Trim over-assigned members** (evaluated in sorted member order):

1. Unconditionally trim any member above `maxQuota` down to `maxQuota`.
2. If a member is above `minQuota`:
   - If fewer than `expectedMaxQuotaMembers` members are at `maxQuota`, allow
     this member to keep `maxQuota` (fill a "maxQuota slot").
   - Otherwise, trim to `minQuota`.  Revoked partitions go to the unassigned pool.

**Phase 2 – Distribute unassigned partitions** (round-robin):

1. First pass: assign to members **below minQuota**.
2. Second pass: assign to members **below maxQuota**.
3. Last resort: any subscribed member (handles unequal subscriptions).

#### Example: 6 partitions, 3 → 4 consumers

```
Before:  C1:[0,1]  C2:[2,3]  C3:[4,5]  C4:[] (new)
minQuota=1, maxQuota=2, expectedMaxQuotaMembers=2

Phase 1:
  C1: 2 partitions, above minQuota → fill maxQuota slot #1 → keep 2 ✓
  C2: 2 partitions, above minQuota → fill maxQuota slot #2 → keep 2 ✓
  C3: 2 partitions, above minQuota → slots full → trim to 1, revoke P5
  C4: 0 partitions, not above minQuota

Phase 2:
  P5 → C4 (below minQuota)

Result:  C1:[0,1]  C2:[2,3]  C3:[4]  C4:[5]
         (2, 2, 1, 1) ✓
```

### Step 5 – Cooperative adjustment

After balancing, identify partitions that are **transfers**: a partition is a
transfer if and only if:

- **(a)** It is **new** for the receiving consumer (assigned but not in their
  `ownedPartitions`), AND
- **(b)** It is **lost** by some other consumer (was in their
  `ownedPartitions` but is not in their new assignment).

Only transfers are removed from the new owner's assignment for this round.
Partitions that are fresh (never owned by anyone) or that stay with the same
consumer are **not** withheld.

In the example above, P5 was owned by C3 and would transfer to C4.  In round 1:
- C4 gets **nothing** (P5 is withheld).
- C3 sees its assignment shrink from [4,5] to [4] → revocation detected.

### Round-2 mechanism (ConsumerGroup.joinAndSync)

When `onAssignment()` detects that a previously-held partition is missing from
the new assignment, it returns `true`, setting `needsCooperativeRejoin = true`
in `ConsumerGroup`.

`joinAndSync()` contains a `while (true)` loop:

```
while (true) {
    needsCooperativeRejoin = false
    JOIN + SYNC
    emit GROUP_JOIN

    if (!needsCooperativeRejoin) break   // done
    // else: immediately do another JOIN + SYNC (round 2)
}
```

In round 2, the previously-revoked partition (P5) is no longer claimed by any
member.  The assigner treats it as unowned → assigns it to C4 with no
cooperative withholding.  `needsCooperativeRejoin` is `false` → the loop exits.

**Convergence guarantee:** For a stable group (no concurrent membership
changes), the protocol converges in exactly 2 rounds.  Round 1 revokes
transferred partitions; round 2 assigns them to their new owners.

---

## Wire format (userData)

```
INT32   generationId
INT32   numTopics
for each topic:
    STRING  topicName        (INT16 length prefix + UTF-8 bytes)
    INT32   numPartitions
    for each partition:
        INT32   partitionId
```

Encoded via `encodeUserData()` / decoded via `decodeUserData()`.  If the buffer
is `null`, empty, or corrupt, the decoder returns defaults
(`generationId = -1, ownedPartitions = {}`).

---

## Key differences from the eager (roundRobin) assigner

| Aspect | roundRobin | cooperativeSticky |
|--------|------------|-------------------|
| Protocol version | 0 | 1 |
| userData | none | generationId + ownedPartitions |
| Revocation | all-at-once (eager) | only transferred partitions |
| Rebalance rounds | 1 | 1–2 |
| Processing gap | all consumers pause | only transferring partitions pause |
| `onAssignment` | not needed | required (tracks state for round 2) |

---

## References

- [KIP-429: Kafka Consumer Incremental Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)
- Java source: `AbstractStickyAssignor.java` → `ConstrainedAssignmentBuilder`
- Java source: `CooperativeStickyAssignor.java`
