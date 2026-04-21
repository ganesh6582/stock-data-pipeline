# stock-data-pipeline



\# Stock Data Engineering Pipeline



\## Overview

Built a serverless data pipeline using AWS services to ingest, process, and query stock market data.



\## Architecture

EventBridge → Lambda → S3 (Bronze \& Silver) → Athena



\## Tech Stack

\- AWS Lambda

\- Amazon S3

\- Amazon Athena

\- EventBridge

\- Python



\## Features

\- Automated daily ingestion

\- Partitioned data lake

\- Bronze \& Silver layers

\- Athena querying



\## How it works

1\. EventBridge triggers Lambda daily

2\. Lambda fetches stock data from API

3\. Stores raw (NDJSON) and processed (CSV) in S3

4\. Updates Athena partitions



\## Key Learnings

\- Serverless architecture

\- Data lake design

\- Partition optimization in Athena

\- Handling Lambda deployment constraints

