# Airlines Scheduling — Streaming + Batch Analytics Pipeline

A small end-to-end prototype that demonstrates how to publish flight batch data to Kafka, consume and persist streaming records (to MongoDB), and run a Spark-based analytics batch job. This repository contains lightweight PySpark scripts and a convenience Bash workflow to glue the pipeline together.

The project is intended for experimentation and demonstration. It assumes a Linux-like runtime for the provided shell script (`main_workflow.sh`). On Windows you can run individual steps from PowerShell or use WSL/Git Bash.

## Repository layout

- `DataFeederService.py` — PySpark job that reads a CSV file and publishes each row as a JSON message to a Kafka topic.
- `StreamIngestionService.py` — (Kafka consumer) PySpark streaming job that consumes from Kafka and writes to MongoDB. (Launched in background by the workflow.)
- `AnalyticsEngine.py` — PySpark batch job that performs feature engineering and trains/evaluates ML models (classification and regression) on the airline dataset.
- `main_workflow.sh` — Orchestration script that starts the streaming consumer, runs the CSV-to-Kafka feeder, then executes the analytics job. Written for Bash (Linux / WSL / Git Bash).

## High level flow

1. Start a Spark streaming consumer (`StreamIngestionService.py`) that reads from Kafka and writes to MongoDB.
2. Run the producer Spark job (`DataFeederService.py`) to read the CSV and publish messages to Kafka.
3. After ingestion completes, run the batch analytics job (`AnalyticsEngine.py`).

## Key assumptions and notes

- The shipped `main_workflow.sh` uses packages that match Spark 2.4.x and Scala 2.11 (see the packages section in the script). If you are using a different Spark/Scala version, update the `--packages` coordinates accordingly.
- `AnalyticsEngine.py` currently reads a CSV directly from a `file:///home/talentum/...` path (it also has a fallback path). The `main_workflow.sh` treats analytics as running after the consumer ingest to MongoDB; to analyze ingested data you should either modify `AnalyticsEngine.py` to read from MongoDB OR modify the workflow/consumer so the CSV remains the canonical input.
- The provided shell workflow is intended for Linux-like environments. On Windows use WSL or run the individual `spark-submit` commands from PowerShell after setting equivalent environment variables.

## Prerequisites

- Java JDK 8 or 11 (required by Spark)
- Apache Spark (the scripts were tested against Spark 2.4.x in the repository notes; newer Spark versions may require changing package coordinates)
- Apache Kafka and Zookeeper (for the Kafka broker)
- MongoDB (if you want to persist streaming data)
- Python 3.7+ (used to run PySpark scripts)
- (Optional) WSL or Git Bash on Windows to run `main_workflow.sh` as-is

Python packages (used by the analytics script):

- tabulate (AnalyticsEngine will attempt to install it at runtime if missing; recommended to install ahead)
- pandas (used for reporting in `AnalyticsEngine.py`)
- pyspark (if running outside Spark-submit you may need pyspark installed for local dev)

Suggested quick installs (on your local dev machine or in a virtualenv):

```bash
# create a venv (optional)
python -m venv .venv; .\.venv\Scripts\Activate.ps1  # PowerShell (Windows)
# or for bash: python3 -m venv .venv; source .venv/bin/activate
pip install pandas tabulate
```

Note: When running under `spark-submit`, Spark supplies the Python interpreter and packages through the Spark environment — you only need local pip installs for unit testing or editor features.

## Environment variables / configuration

The scripts read configuration from environment variables. The following are important:

- `KAFKA_BROKERS` — Kafka bootstrap servers. Default in scripts: `localhost:9092`.
- `KAFKA_TOPIC` — Kafka topic name. Default: `flight_live_data`.
- `CSV_PATH` — Path to the CSV dataset that `DataFeederService.py` will read. Default in `DataFeederService.py` is `/home/talentum/airlines_extended_15000.csv`.
- `SPARK_HOME` — Path to your Spark installation (used by `main_workflow.sh`).

You can export these variables in Linux/WSL or set them in PowerShell prior to running commands.

## Running the pipeline (recommended: Linux/WSL/Git Bash)

Edit `main_workflow.sh` and set the three main variables at the top if needed:

- `KAFKA_BROKERS`
- `KAFKA_TOPIC`
- `SPARK_HOME`
- `CSV_PATH` (path to `airlines_extended_15000.csv`)

Then run from a Bash shell:

```bash
chmod +x main_workflow.sh
./main_workflow.sh
```

The script will:

- launch `StreamIngestionService.py` (consumer) in the background using `spark-submit`,
- run `DataFeederService.py` to publish CSV rows to Kafka,
- run `AnalyticsEngine.py` once ingestion is complete,
- clean up background consumer process.

### Running steps manually (PowerShell on Windows)

If you prefer running steps one-by-one (or you are on Windows without WSL), run the `spark-submit` commands directly from PowerShell. Example (replace `C:\spark` with your Spark home and update the CSV path):

```powershell
# $env:KAFKA_BROKERS = "localhost:9092"
# $env:KAFKA_TOPIC = "flight_live_data"
# $env:CSV_PATH = "C:/path/to/airlines_extended_15000.csv"
# $env:SPARK_HOME = "C:/spark"

& "$env:SPARK_HOME\bin\spark-submit" `
  --conf "spark.pyspark.python=$env:PYTHON" `
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,com.databricks:spark-csv_2.11:1.4.0" `
  DataFeederService.py
```

Use a similar pattern to start the consumer (`StreamIngestionService.py`) and the analytics job (`AnalyticsEngine.py`). Update the `--packages` list to match the Spark/Scala version you have installed.

## Common issues & troubleshooting

- Missing CSV / FileNotFound: `DataFeederService.py` and `AnalyticsEngine.py` expect a CSV at configured `CSV_PATH` or at the hard-coded `/home/talentum/...` fallback paths. Update `CSV_PATH` or edit the Python scripts to point to your local file path.
- Package/Version mismatch: The `main_workflow.sh` packages target Spark 2.4.x + Scala 2.11. If your Spark distribution uses Scala 2.12 or Spark 3.x, use the appropriate connector versions (for example `spark-sql-kafka-0-10_2.12` for Scala 2.12).
- Windows vs Linux path handling: Paths in the Python scripts (e.g., `file:///home/talentum/...`) are Linux-style. Use WSL or change the path to a Windows `file:///C:/...` style when necessary.
- Kafka connectivity: Ensure Kafka and Zookeeper are running and reachable at `KAFKA_BROKERS`. Use `kafka-topics.sh --bootstrap-server localhost:9092 --list` to verify.
- MongoDB writing: Ensure the consumer (`StreamIngestionService.py`) can reach MongoDB and has the correct connection URI. If ingestion is not happening, check the consumer logs and the configured MongoDB host/port.

## Development notes and potential improvements

- Make `AnalyticsEngine.py` read from MongoDB (or accept a CLI flag) so batch analysis runs on ingested data instead of the original CSV when the pipeline is executed end-to-end.
- Add a `requirements.txt` and/or containerize the pipeline with Docker Compose (Kafka, Zookeeper, MongoDB, Spark driver) for easier local testing.
- Add unit tests for smaller logic pieces and lightweight integration tests for the pipeline.

## Minimal contract (what each script expects / produces)

- DataFeederService.py
  - Inputs: CSV file with flight statistics (path via `CSV_PATH`).
  - Outputs: Writes JSON messages to Kafka topic (`KAFKA_TOPIC`).
  - Errors: Fails if Spark cannot read the CSV or cannot connect to Kafka.

- StreamIngestionService.py
  - Inputs: Kafka topic messages.
  - Outputs: Persists records to MongoDB.
  - Errors: Fails/loops if cannot reach Kafka or MongoDB.

- AnalyticsEngine.py
  - Inputs: CSV file (currently) at `/home/talentum/...` or the configured `csv_path` paths in the script.
  - Outputs: Trains models and prints evaluation reports to stdout.
  - Errors: Fails if the CSV file is not present or if Spark cannot start.

## Next steps (recommended)

1. Create a `requirements.txt` listing Python packages used by `AnalyticsEngine.py`.
2. Decide whether analytics should consume from MongoDB and modify `AnalyticsEngine.py` accordingly.
3. Add a `docker-compose.yml` for a local test environment (Zookeeper, Kafka, MongoDB).

--
This README was created to describe the repository as provided. If you'd like, I can also:

- add a `requirements.txt` file now,
- produce a `docker-compose.yml` for Kafka + Zookeeper + MongoDB,
- edit `AnalyticsEngine.py` to read from MongoDB instead of a local CSV.

If you want any of the above done now, tell me which one and I'll implement it next.
