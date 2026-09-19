# MatchFlow: Airflow ETL for Cricket Match Data

A Docker Compose learning project that generates synthetic cricket match data, archives the CSV in MinIO, and loads transformed records into MySQL and MongoDB. Apache Airflow orchestrates tasks with CeleryExecutor, Redis, and PostgreSQL; Flower monitors workers, and Slack operators provide notifications.

**Current status:** known data-integrity and notification bugs need attention. Read [Known issues](#known-issues) before running. Use disposable development data: each run deletes the existing `backup` bucket and drops the MySQL target table.

## Architecture

![MatchFlow ETL architecture](screenshots/Architecture.png)

## Pipeline

The DAG ID is `simple_etl_dag`. It runs manually (`schedule_interval=None`), with catchup disabled. Each run generates 2,000 synthetic records and overwrites `data/mockup_data.csv`; it does not download real IPL results.

```mermaid
flowchart LR
    A[start_task] --> B[extract_task]
    B --> C[file_sensor_task]
    C --> D[copy_to_s3_task]
    D --> E[transform_task]
    E --> F[mysql_task]
    E --> G[mongo_task]
    F --> H[determine_notification_path]
    G --> H
    H --> I[success_path]
    H --> J[failure_path]
    I --> K[end_task]
    J --> K
```

The diagram shows the wired dependencies; failure routing currently needs correction.

| Stage | Current behavior |
| --- | --- |
| Generate | Faker and Python's `random` module create match IDs, captains, teams, winners, venues, and dates. |
| Wait | `FileSensor` checks `/opt/airflow/data/mockup_data.csv` every 15 seconds, with a 60-second timeout. |
| Archive | Recreates the MinIO `backup` bucket and uploads `mockup_data.csv`. |
| Transform | Adds `toss_decision_impact` for MySQL; creates nested `teams`, `is_upset`, and `venue_type` fields for MongoDB. Both record lists pass through XCom. |
| Load | Recreates `airflow_db.cricket_matches` in MySQL and appends documents to `testdb.test_collection` in MongoDB. |
| Notify | Branches to Slack notification tasks. Failure detection is currently unreliable. |

## Repository layout

```text
matchflow-airflow-etl/
|-- dags/simple_etl_dag.py        # DAG, loaders, and notifications
|-- scripts/mockup_data.py       # Synthetic CSV generation
|-- scripts/transflow_ipl_data.py # Destination transformations
|-- scripts/__init__.py
|-- data/mockup_data.csv         # Included sample; overwritten by the DAG
|-- logs/                       # Bind-mounted Airflow logs
|-- plugins/                    # Placeholder for custom plugins
|-- screenshots/                # Example UI and output screenshots
|-- Dockerfile.airflow          # Base image: apache/airflow:2.8.2
|-- docker-compose.yaml         # Airflow and supporting services
|-- requirements.txt            # Python dependencies (currently unpinned)
|-- my.cnf                      # Not mounted by Compose
|-- .gitignore
`-- README.md
```

## Local setup

### Prerequisites

- Docker Engine or Docker Desktop running Linux containers, with Docker Compose v2 (`docker compose`).
- Git and internet access for images and Python packages.
- Sufficient memory for the Airflow services and supporting databases; 8 GB allocated to Docker is a reasonable starting point for this stack.
- Available host ports: `8080`, `5555`, `9000`, `9001`, `3307`, `27017`, `5432`, and `6379`.

The Compose file contains development credentials and publishes database ports. Keep this stack on a trusted development machine.

### 1. Clone the repository

```sh
git clone https://github.com/logarajeshwarankarthikeyan/matchflow-airflow-etl.git
cd matchflow-airflow-etl
```

### 2. Configure the Airflow user

Create a `.env` file beside `docker-compose.yaml`, or merge these settings into an existing one:

```dotenv
AIRFLOW_UID=50000
AIRFLOW_GID=0
```

On Linux, use your host user ID (the output of `id -u`) for `AIRFLOW_UID`. This project's Compose file defaults the group to `50000`; explicitly setting `AIRFLOW_GID=0` follows the Airflow image's group convention and helps avoid volume permission problems. Ensure the mounted `dags/`, `logs/`, `plugins/`, `data/`, and `scripts/` directories are accessible to the container user, with write access to `data/` and `logs/`. See the [Airflow 2.8.2 Docker setup guide](https://airflow.apache.org/docs/apache-airflow/2.8.2/howto/docker-compose/index.html).

### 3. Build and initialize

```sh
docker compose config --quiet
docker compose build
docker compose up airflow-init
```

Wait for `airflow-init` to exit with code `0`. It initializes the metadata database and creates the Airflow administrator. Run initialization separately because the long-running services do not depend on its successful completion in this Compose file.

The base image is Airflow `2.8.2`, but the unconstrained dependency installation can change the installed version. A successful build alone does not establish compatibility; see [Known issues](#known-issues).

### 4. Start and inspect services

```sh
docker compose up -d
docker compose ps
docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Check for unexpected version changes or DAG import errors before triggering the pipeline. `airflow-init` being exited with code `0` is normal.

| Service | Host address | Default credentials / purpose |
| --- | --- | --- |
| Airflow | [localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
| Flower | [localhost:5555](http://localhost:5555) | Celery monitoring; no authentication configured |
| MinIO console | [localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| MinIO S3 API | `http://localhost:9000` | S3 endpoint; console is on `9001` |
| MySQL | `localhost:3307` | Database `airflow_db`; `airflow_user` / `airflow_pass` |
| MongoDB | `localhost:27017` | `mongoadmin` / `secret`; authentication database `admin` |
| PostgreSQL | `localhost:5432` | Airflow metadata database `airflow`; `airflow` / `airflow` |
| Redis | `localhost:6379` | Celery broker; no password configured |

Airflow's administrator credentials can be overridden before initialization through `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD`. MySQL's development root password is `rootpassword`.

## Airflow connections

In the UI, open **Admin > Connections**. Inside Docker, use Compose service names and container ports, not `localhost` or the host-side MySQL port `3307`.

### File sensor

- **Connection ID:** `fs_connection_id`
- **Connection type:** File (path)
- **Extra:** `{"path": "/opt/airflow/data"}`

### MySQL

- **Connection ID:** `mysql_default`
- **Connection type:** MySQL
- **Host:** `mysql`
- **Schema:** `airflow_db`
- **Login:** `airflow_user`
- **Password:** `airflow_pass`
- **Port:** `3306`

### MinIO

- **Connection ID:** `minio_bucket_connection`
- **Connection type:** Amazon Web Services
- **AWS Access Key ID / Login:** `minioadmin`
- **AWS Secret Access Key / Password:** `minioadmin`
- **Extra:**

```json
{
  "endpoint_url": "http://minio:9000",
  "region_name": "us-east-1",
  "config_kwargs": {
    "s3": {"addressing_style": "path"}
  }
}
```

The endpoint belongs in `Extra`. The DAG creates the bucket. Airflow's AWS connection test can fail against MinIO because it checks AWS STS, which MinIO may not implement; this alone does not establish that S3 access is broken. See the [Amazon provider connection documentation](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html).

### Slack

Create a Slack app with Incoming Webhooks enabled, add a webhook to your chosen workspace channel, and configure:

- **Connection ID:** `slack_default`
- **Connection type:** Slack Incoming Webhook
- **Webhook Token / Password:** the full incoming webhook URL
- **Host and Schema:** leave blank when using the full URL in Password

See the [Slack provider connection documentation](https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/connections/slack-incoming-webhook.html). Keep the webhook secret out of source control. Select the destination channel when creating the webhook; modern Slack incoming webhooks do not honor the DAG's channel, username, or icon overrides.

Both wired notification tasks use `slack_default`. A separate `slack_success_notification` object references `slack_webhook`, but it has no DAG assignment or dependencies and is not part of the pipeline. Creating that extra connection does not fix failure routing.

### MongoDB

The loader does not consume an Airflow MongoDB connection. It uses this hardcoded URI:

```text
mongodb://mongoadmin:secret@mongo:27017/testdb?authSource=admin
```

Creating or editing `mongo_default` in the UI has no effect until the loader is changed to use it.

## Run and inspect the pipeline

After configuring connections, unpause `simple_etl_dag` and trigger it in the UI, or run:

```sh
docker compose exec airflow-scheduler airflow dags unpause simple_etl_dag
docker compose exec airflow-scheduler airflow dags trigger simple_etl_dag
```

Avoid overlapping runs: they share the same CSV, bucket, and MySQL table. Inspect individual task logs and output counts; the final DAG status currently can hide upstream failures.

- **CSV:** `data/mockup_data.csv` on the host.
- **MinIO:** `backup/mockup_data.csv` in the console.
- **MySQL:** connect with `docker compose exec mysql mysql -u airflow_user -p airflow_db`, enter the password when prompted, then query:

  ```sql
  SELECT COUNT(*) FROM cricket_matches;
  SELECT * FROM cricket_matches LIMIT 10;
  ```

- **MongoDB:** connect using a MongoDB client at `localhost:27017`, with the credentials above and `authSource=admin`; inspect `testdb.test_collection`. In a Mongo shell:

  ```javascript
  db.getSiblingDB("testdb").test_collection.countDocuments({})
  db.getSiblingDB("testdb").test_collection.findOne()
  ```

The supplied CSV has 2,000 rows but only eight distinct `match_id` values. The current MySQL upserts collapse these to eight records. MongoDB appends documents, so its count grows across runs and repeated loads.

## Known issues

These are implementation findings, not fixes included in this README update.

| Priority | Finding and impact | Suggested correction |
| --- | --- | --- |
| High | `scripts/mockup_data.py` generates IDs only from 2 through 9. MySQL's primary-key upserts overwrite earlier matches. | Generate unique match IDs and validate uniqueness before loading. |
| High | `upload_to_s3()` deletes every object in `backup`, then recreates the bucket. A failed upload can leave no backup. | Preserve the bucket and use run-specific object keys. |
| High | `load_to_mysql()` drops `cricket_matches` before loading. Previous results are lost, and a failed load can leave an empty table. | Validate a staging table before replacement, or use deliberate incremental upserts. |
| High | `_determine_branch()` reads XCom return values as task states, although the loaders return no status. Its default `all_success` rule prevents it from running after a loader failure; `failure_path` checks the branch with `one_failed`, not the loaders. | Inspect actual task states after completion or use correctly wired failure callbacks/tasks. Replace `{{ ti.exception }}` with a supported error source. |
| High | The sole leaf, `end_task`, uses `all_done`, so it can succeed after upstream failures and make the whole run appear successful. | Preserve failure in the terminal task/status logic. See [Airflow's DAG run status rules](https://airflow.apache.org/docs/apache-airflow/2.8.2/core-concepts/dag-run.html). |
| High | The Dockerfile installs unpinned providers/dependencies without constraining Airflow. The resolved version may differ from the `2.8.2` base and break imports or Compose commands. | Pin Airflow during installation and select compatible dependency constraints. See [Airflow's image-building guidance](https://airflow.apache.org/docs/docker-stack/build.html). |
| Medium | MongoDB uses `InsertOne` without a stable unique match key, so rerunning a load appends duplicates. | Define a unique key and use idempotent upserts after fixing ID generation. |
| Medium | Concurrent DAG runs overwrite shared files and destinations; no `max_active_runs=1` is set. | Serialize runs initially, or isolate files and destination writes by run. |
| Medium | `is_upset` tests whether the winner is neither participating team, so it is always false for generated matches. `venue_type` uses the word count of a generated city name. | Define meaningful domain rules and supply the data needed to evaluate them. |
| Medium | MongoDB credentials are embedded in code; Compose uses an empty Fernet key and development passwords. | Use Airflow connections/secrets and configure encryption before storing real credentials. |

Additional cleanup: close MySQL connections/cursors on success and failure; validate CSV columns, types, and empty inputs; configure retries after making writes idempotent; remove the disconnected Slack operator; pin floating MySQL, Redis, and MinIO image tags. `my.cnf` is unused, and Compose's top-level `version` field produces an obsolete-field warning. For larger datasets, pass file/object references instead of full record lists through XCom.

## Troubleshooting and shutdown

```sh
docker compose ps
docker compose logs --tail=100 airflow-init airflow-scheduler airflow-worker
docker compose exec airflow-scheduler airflow dags list-import-errors
```

- **Cannot connect to Docker:** start Docker Desktop/Engine with Linux containers.
- **Airflow startup/import errors:** check initialization logs, the installed Airflow version, and dependency compatibility.
- **Permission errors:** check `AIRFLOW_UID`, set `AIRFLOW_GID=0`, and verify bind-mount access.
- **Connection failures:** verify exact connection IDs and container hostnames (`mysql`, `mongo`, `minio`).
- **Missing failure alerts or unexpectedly green runs:** inspect individual task states and the notification issues above.
- **Changes to `my.cnf` have no effect:** mount it into MySQL's configuration directory before expecting it to be used.
- **Changed initialization passwords do not take effect:** existing database volumes retain initialized users; update the users in the database deliberately.

Stop the stack while keeping named-volume data:

```sh
docker compose down
```

`docker compose down --volumes` also deletes the named volumes holding PostgreSQL metadata, MySQL data, MongoDB data, and MinIO objects. Use it only for an intentional disposable-environment reset. Host bind-mounted files, including `data/mockup_data.csv`, remain.

## Screenshots

These are included examples, not proof that current dependencies or every failure path have been verified.

<details>
<summary>View pipeline, service, and output screenshots</summary>

### Airflow DAG

![Airflow DAG](screenshots/airflow_dag.png)

### Slack notification

![Slack notification](screenshots/slack_notification.png)

### MinIO upload

![MinIO upload](screenshots/minio_upload.png)

### Container status

![Docker Compose status](screenshots/docker_compose_ps.png)

### Container logs

![Docker Compose logs](screenshots/docker_compose_logs.png)

### MySQL output

![MySQL output](screenshots/mysql_output.png)

### MongoDB output

![MongoDB output](screenshots/mongodb_output.png)

</details>

## Validation scope

The review covered Python source, Compose/Docker configuration, dependencies, and the supplied CSV. Python syntax parsing and Compose configuration validation passed. Both transformations processed all 2,000 sample rows and produced JSON-serializable records. An isolated loader check with a simulated SQL cursor confirmed that the sample retains only eight primary keys; an isolated branch check returned `success_path` when task return XCom values were absent.

The full container pipeline was not executed: the Docker daemon was unavailable, and Airflow and Faker were not installed in the local Python environment. There is no automated test suite in this repository.

## License

No repository-level `LICENSE` file is currently included. The Compose file retains an Apache Software Foundation license header; that header alone does not establish a license for the entire project.
