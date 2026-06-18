# Architecture

```mermaid
flowchart LR
    A[Raw betting data<br/>source or landing area] --> B[Scheduler or job trigger<br/>runs batch workflow]
    C[Metadata, schema contract,<br/>configuration and feature definitions] --> B

    B --> D[Validation job]
    D -->|valid| E[Curated validated bets<br/>valid_bets.parquet]
    D -->|invalid| F[Invalid-record quarantine<br/>and review path]

    E --> G[Customer feature<br/>generation job]
    G --> H[Versioned feature output<br/>feature store or serving table]

    H --> I[Batch model training]
    H --> J[Batch scoring]
    H --> K[BI, CRM,<br/>or decisioning]

    B --> L[Logging, monitoring,<br/>alerting and run manifest]
    F --> M[Correction path<br/>fix source data]
    M --> A
    L --> N[Rerun or backfill]
    N --> B
```

The local package implements the scheduled batch job. It validates raw bets, quarantines bad records, writes curated parquet, builds versioned customer features, and records enough metadata for monitoring, reruns, and downstream ML consumers.
