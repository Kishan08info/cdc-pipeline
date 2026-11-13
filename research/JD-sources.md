# Theme source — Nov 13, 2026 JD pools

Pool files: `~/workspace/job-search/pool-2026-11-13-de.md`,
`~/workspace/job-search/pool-2026-11-13-ai.md`.

DE themes that pointed at CDC:
- **M&T Bank — Senior Data Engineer** (official Workday page read live):
  Kafka/CDC listed as preferred; core stack Snowflake/dbt/Spark (a stack
  gap, not a fit — but CDC itself is squarely in his Kafka/streaming lane).
- **Solve Intelligence — Data Engineer** (YC, "Full visa sponsorship"):
  "large-scale resumable pipelines" + document processing + keyword/vector/
  structured search over tens to hundreds of millions of records.
  Resumable pipelines = the checkpoint/resume mechanism built here.
- **Flow Traders — Core Data Engineer** ($165–210K, Built In copy):
  data-quality practice — "tests, contracts, lineage, observability".
  The reconciliation step is the quality gate for this pipeline.

Why CDC and not the other live themes:
- Search systems (Solve Intelligence) overlaps RAG retrieval (projects 1,
  3, 17, 18, 19) — less distinct.
- Modern data stack (dbt/Snowflake/Databricks/Spark/Airflow) is already
  covered by projects 11 and 16 with honest local stand-ins.
- No shipped project covered change capture: project 13 did incremental
  loads via an API watermark (no deletes, no ordering); project 14 consumed
  CDC feeds but never built the apply layer; project 12 was a streaming
  consumer with no deletes or resume. CDC is the uncovered theme.

Queued ideas not taken this run: LoRA vs QLoRA vs full fine-tune shootout
(needs GPU training time; model-training roles are outside his target mix)
and voice-enabled receptionist (project 4 upgrade; still queued).
