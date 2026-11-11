# Choose Databricks Compute by Workload

Video: [youtu.be/jB1ayvonDas](https://youtu.be/jB1ayvonDas)

Companion to [A01 Platform tour](../../lessons/associate-certification/a01-platform-tour/).

Choose Databricks compute by the workload. Interactive notebooks use serverless notebook compute, or all-purpose compute when you need a custom configuration. Scheduled production uses serverless jobs or classic jobs compute. Structured Query Language (SQL) analytics, business intelligence (BI) tools, and dashboards use a SQL warehouse, ideally serverless. Scheduled production does not belong on an all-purpose cluster, because jobs compute has a lower Databricks Unit (DBU) rate. If classic jobs must start fast, back them with a pool. Free Edition gives you serverless notebooks and one SQL warehouse, so classic compute is an exam concept here.

## Memory trick

When you read a scenario, look for the workload words first. Interactive, scheduled, SQL, startup time, and cost.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
