---
slug: spark-big-query-partitioning
title: Handling Dynamic Partitioning with Spark On BigQuery
author: Hayssam Saleh
author_title: Starlake Core Team Member
author_url: https://www.linkedin.com/in/hayssams/
author_image_url: https://s.gravatar.com/avatar/04aa2a859a66b52787bcba8c36beba8c.png
tags: [Spark, BigQuery, Dataproc, Google Cloud]
---


# The problem
When loading data into BigQuery, you may want to:
- Overwrite all the existing data and replace it with the incoming data.
- Append
