# GameArena — Real-Time Gaming Leaderboard System

## Project Overview

GameArena is a large-scale gaming leaderboard system designed for millions of concurrent players.  
It implements the full pipeline shown in the architecture diagram:

```
Game Clients → API Gateway → WebSocket Manager → Score Validator
    → Apache Kafka → Ranking Engine / Fraud Detection / Analytics
    → Redis (HOT) / PostgreSQL (WARM) / Cassandra (COLD)
    → Leaderboard APIs → Global / Regional / Season Rankings
```

The Python implementation covers all six case study questions:

| Question | Topic | Key Class / Module |
|----------|-------|--------------------|
| Q1 | Requirements Analysis | (see notebook) |
| Q2 | System Architecture | `GameArenaPipeline` |
| Q3 | Score Processing & Ranking | `ScoreValidator`, `LeaderboardManager` |
| Q4 | Database Design | `Player`, `ScoreEvent`, schema docs |
| Q5 | Ranking Algorithm | `SortedLeaderboard` |
| Q6 | Scalability & Fault Tolerance | `LeaderboardCache`, `DeadLetterQueue`, `EventQueue` |

---

## Project Structure

```
leaderboard_project/
├── leaderboard_system.py      # Complete Python implementation
├── GameArena_Leaderboard.ipynb # Jupyter notebook with Q1–Q6 walkthrough
└── README.md                  # This file
```

---

## Dependencies

The implementation uses **Python standard library only** — no external packages required.

| Module | Usage |
|--------|-------|
| `uuid` | Unique event IDs for idempotency |
| `hmac`, `hashlib` | HMAC-SHA256 signature validation |
| `threading` | Thread-safe concurrent operations |
| `collections` | `defaultdict`, `deque` for queues |
| `dataclasses` | Clean data model definitions |
| `heapq` | Heap operations for ranking |
| `time`, `random`, `logging` | Utilities |

**Optional (for production extension):**
```
redis-py       # Replace SortedLeaderboard with real Redis ZADD
kafka-python   # Replace EventQueue with real Kafka producer/consumer
psycopg2       # Connect to real PostgreSQL
cassandra-driver # Connect to real Cassandra
```

---

## Setup Instructions

### 1. Prerequisites
- Python 3.10 or higher
- Jupyter Notebook (optional, for `.ipynb`)

### 2. Clone / Download
```bash
# Place both files in the same directory
leaderboard_system.py
GameArena_Leaderboard.ipynb
README.md
```

### 3. Install Jupyter (if needed)
```bash
pip install notebook
```

---

## Execution Steps

### Option A — Run the Python script directly
```bash
python leaderboard_system.py
```

Expected output:
```
[SETUP] 8 players registered.
[INGEST] Sending normal score events...
[OK] Processed batch. Stats: {...}

GLOBAL TOP-10
──────────────────────────────────────────────────────
  Rank   Username         Score  Region
  1      NeonRaider      102000  NA
  2      ShadowStrike    110000  ASIA
  ...

[FRAUD] Sending tampered (bad HMAC) event for p004...
[FRAUD] Sending velocity burst (60 events) for p002...
[DONE] Final stats: {...}
```

### Option B — Run the Jupyter Notebook
```bash
jupyter notebook GameArena_Leaderboard.ipynb
```
Then run cells sequentially (Shift+Enter or "Run All").

---

## Additional Project Details

### Architecture Decisions

**Why Redis Sorted Sets for ranking?**  
`ZADD` is O(log N) and `ZRANK` is O(log N) in production Redis, giving sub-millisecond rank  
lookups even with 10M+ players. The Python `SortedLeaderboard` class simulates this behavior.

**Why Kafka for event streaming?**  
Kafka's partitioning ensures events for the same player are always processed in order  
(using `player_id` as the partition key). This prevents rank inconsistencies from  
out-of-order processing. The `EventQueue` class simulates Kafka's partition model.

**Why HMAC validation?**  
Game servers sign each score event with a shared secret. The `ScoreValidator` verifies  
this signature before any processing, preventing score injection from external clients.

**Why a Dead Letter Queue?**  
Failed events (invalid HMAC, fraud flags, processing errors) are captured in the DLQ  
rather than silently dropped. Operators can review and replay them after investigation.

### Key Algorithms

| Algorithm | Location | Complexity |
|-----------|----------|------------|
| Max-score update | `SortedLeaderboard.update_score()` | O(log N) |
| Top-N retrieval | `SortedLeaderboard.get_top_n()` | O(N log N) |
| Z-score anomaly | `FraudDetectionEngine._zscore_anomaly()` | O(H) where H = history size |
| Velocity detection | `FraudDetectionEngine._velocity_anomaly()` | O(W) where W = window size |
| Consistent hash partition | `EventQueue._partition_for()` | O(1) |

### Comparison with Real-World Systems

| Platform | Score Store | Streaming | History |
|----------|-------------|-----------|---------|
| Steam | PostgreSQL + Redis | Internal queue | PostgreSQL |
| Riot Games (LoL) | Custom in-memory | Kafka | Cassandra |
| **GameArena (this project)** | Redis Sorted Sets | Kafka (simulated) | Cassandra (simulated) |

### Extending to Production

To connect to real infrastructure, replace:

```python
# Replace SortedLeaderboard with:
import redis
r = redis.Redis()
r.zadd('leaderboard:global', {player_id: score})
r.zrevrank('leaderboard:global', player_id)

# Replace EventQueue with:
from kafka import KafkaProducer, KafkaConsumer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('score-events', value=event_json)
```

---

*Submitted for: Case Study — Designing a Gaming Leaderboard System*
