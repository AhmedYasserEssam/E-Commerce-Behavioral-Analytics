# Hadoop HDFS Data Loading Guide

## Overview

This document records the workflow used to start the local Hadoop Docker cluster and upload the `ecommerce_logs.csv` dataset into HDFS for the **E-Commerce Behavioral Analytics** project.

## Environment

- Project root: `C:\Work\University\E-Commerce Behavioral Analytics`
- Docker Compose directory: `docker-hadoop`
- Docker Compose version: `v2.39.4-desktop.1`
- Source dataset: `data/raw/ecommerce_logs.csv`
- HDFS destination: `/user/hadoop/ecommerce_input/ecommerce_logs.csv`

## Environment Setup

Before starting the Hadoop cluster or running the Spark job, make sure your local environment is ready from the project root.

### Common requirements

- Install Python 3
- Install Docker with Docker Compose support
- Open a terminal in the project root
- Install the project dependencies from `requirements.txt`

Verify Docker Compose is available:

```bash
docker compose version
```

### Windows

Use PowerShell from the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Notes:

- If script execution is blocked, run PowerShell as a user who can enable local script execution, or use your existing virtual environment workflow.
- If `spark-submit` is not available in native PowerShell, use the activated environment where `pyspark` is installed, or run the Spark step from WSL.

### Linux

Use a shell such as `bash` from the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### macOS

Use Terminal or iTerm from the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Quick verification

After setup, these commands should work:

```bash
python --version
pip --version
docker compose version
```

## Localhost Web Previews

After `docker compose up -d`, the following Hadoop web interfaces are available from your local machine:

- NameNode: `http://localhost:9870/`
- DataNode: `http://localhost:9864/`
- Resource Manager: `http://localhost:8088/`

The following services are exposed by Docker Compose, but they are not pinned to fixed localhost ports in the current `docker-hadoop/docker-compose.yml`:

- NodeManager: container port `8042`
- History Server: container port `8188`

To discover their current localhost URLs, run:

```bash
docker compose -f docker-hadoop/docker-compose.yml port nodemanager 8042
docker compose -f docker-hadoop/docker-compose.yml port historyserver 8188
```

Then open the returned host ports in your browser:

- NodeManager: `http://localhost:<mapped-port>/node`
- History Server: `http://localhost:<mapped-port>/applicationhistory`

## Procedure

### 1. Run Docker Compose from the correct directory

Running `docker-compose down` from the project root failed because there is no Compose file at that level:

```bash
docker-compose down
```

Expected message:

```text
no configuration file provided: not found
```

Move into the directory that contains `docker-compose.yml`:

```bash
cd docker-hadoop
```

### 2. Stop any existing Hadoop containers

Bring down any existing cluster before starting a fresh one:

```bash
docker compose down
```

Note:
Docker displayed a warning that the `version` field in `docker-compose.yml` is obsolete. This warning does not block execution, but the field can be removed later to avoid confusion.

### 3. Start the Hadoop cluster

Launch the Hadoop services in detached mode:

```bash
docker compose up -d
```

Successful startup should create the Docker network, required volumes, and start these containers:

- `namenode`
- `datanode`
- `resourcemanager`
- `nodemanager`
- `historyserver`

### 4. Confirm Docker Compose is available

The environment used in this run reported:

```bash
docker compose version
```

Output:

```text
Docker Compose version v2.39.4-desktop.1
```

### 5. Inspect the source dataset

Return to the project root and preview the CSV file:

```bash
cd ..
head -5 data/raw/ecommerce_logs.csv
```

This confirms the file exists and shows the expected header:

```text
timestamp,session_id,user_id,event_type,product_id,price,referrer,user_metadata,product_metadata
```

### 6. Start the cluster again before loading data

After verification, move back to the Compose directory and ensure the cluster is running:

```bash
cd docker-hadoop
docker compose up -d
cd ..
```

### 7. Copy the dataset into the NameNode container

Copy the source CSV file from the host machine into the `namenode` container:

```bash
docker cp data/raw/ecommerce_logs.csv namenode:/tmp/ecommerce_logs.csv
```

Successful transfer message:

```text
Successfully copied 2.15GB to namenode:/tmp/ecommerce_logs.csv
```

### 8. Create the target HDFS directory

Create the destination directory inside HDFS:

```bash
docker exec -it namenode hdfs dfs -mkdir -p /user/hadoop/ecommerce_input
```

### 9. Upload the file into HDFS

Put the copied CSV file into HDFS:

```bash
docker exec -it namenode hdfs dfs -put /tmp/ecommerce_logs.csv /user/hadoop/ecommerce_input/
```

During upload, informational SASL messages may appear similar to:

```text
INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
```

These messages were observed during the successful upload and do not indicate failure by themselves.

### 10. Verify the uploaded file in HDFS

List the target HDFS directory and confirm the file is present:

```bash
docker exec -it namenode hdfs dfs -ls -h /user/hadoop/ecommerce_input/
```

Expected result:

```text
Found 1 items
-rw-r--r--   3 root supergroup      2.0 G 2026-05-09 15:56 /user/hadoop/ecommerce_input/ecommerce_logs.csv
```

## Final Status

The dataset was successfully uploaded to:

```text
/user/hadoop/ecommerce_input/ecommerce_logs.csv
```

## Running the Spark Job

### 1. Do not run `spark-submit` inside the `namenode` container

This command fails:

```bash
docker exec -it namenode spark-submit /tmp/01_market_basket.py
```

Reason:
The `namenode` container does not include Spark binaries, so `spark-submit` is not available there.

### 2. Run Spark from the project environment

Use the host or WSL virtual environment:

```bash
spark-submit src/01_market_basket.py
```

### 3. Use the HDFS RPC endpoint

The Spark script must read from:

```text
hdfs://localhost:9000
```

and not:

```text
hdfs://localhost:9870
```

Reason:
Port `9000` is the HDFS RPC service. Port `9870` is only the NameNode web UI.

### 4. Enable hostname-based DataNode access for local Spark

When Spark runs outside Docker, it needs to connect to the DataNode using a host-reachable hostname rather than the container's internal Docker IP.

Fixes applied in this project:

- Exposed DataNode ports `9864` and `9866` in `docker-hadoop/docker-compose.yml`
- Configured Hadoop to advertise `localhost` as the DataNode hostname in `docker-hadoop/hadoop.env`
- Configured the DataNode to bind explicitly on `0.0.0.0:9866` and `0.0.0.0:9864`
- Configured the Spark job to set `spark.hadoop.dfs.client.use.datanode.hostname=true`

### 5. Restart the cluster after Hadoop configuration changes

After editing `docker-compose.yml` or `hadoop.env`, recreate the cluster:

```bash
cd docker-hadoop
docker compose down
docker compose up -d
cd ..
```

Then re-run:

```bash
spark-submit src/01_market_basket.py
```

## Checking the Output

### 1. Verify that the output directory exists

Use the following command to confirm that Spark wrote the output files to HDFS:

```bash
docker exec -it namenode hdfs dfs -ls /user/hadoop/market_basket_output
```

Expected result:
You should see `_SUCCESS` and one or more `part-*` files.

### 2. Preview the result content

To display the first lines of the generated output, run:

```bash
docker exec -it datanode hdfs dfs -cat /user/hadoop/market_basket_output/part-* | head -20
```

### 3. Optional web-based verification

You can also inspect the output from the NameNode web interface at:

```text
http://localhost:9870
```

Then browse to:

```text
/user/hadoop/market_basket_output
```
