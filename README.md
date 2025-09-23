# Fault-Tolerant Key/Value Store with Raft and Snapshots

A key/value store built on the Raft consensus algorithm. Supports `Get` and `Put` operations with log compaction through snapshots.

## Features

- **Key/Value Store**
    - In-memory storage with linearizable `Get`, `Put` operations.

- **Client Deduplication**
    - Prevents re-execution of duplicate requests.

- **Raft Integration**
    - All operations go through Raft for replication and agreement.

- **Snapshotting**
    - Creates snapshots when Raft state grows beyond `maxraftstate`.
    - Snapshots store key/value data and client deduplication info.
    - Restores from snapshots without replaying logs.

## Implementation

- **Command Application**
    - A goroutine applies committed Raft log entries to the state machine.

- **Snapshot Creation**
    - Encodes state with `labgob` and calls `rf.Snapshot(lastAppliedIndex, snapshot)`.

- **Snapshot Installation**
    - On receiving a snapshot from Raft, the state machine restores it directly.