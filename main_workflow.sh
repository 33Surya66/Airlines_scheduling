#!/bin/bash

# --- Configuration ---
# Set your core variables
KAFKA_BROKERS="localhost:9092"
KAFKA_TOPIC="flight_live_data"
SPARK_HOME="/home/talentum/spark"
CSV_PATH="/home/talentum/shared/airlines_extended_15000.csv" 
PYTHON_EXECUTABLE=$(which python3)

# --- Dependency Maven Coordinates (For --packages) ---
# NOTE: Use Scala 2.11 and Spark 2.4.x compatible versions
MONGO_PACKAGE="org.mongodb.spark:mongo-spark-connector_2.11:2.4.0"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0"
CSV_PACKAGE="com.databricks:spark-csv_2.11:1.4.0" 

# --- Composite Packages for Each Step ---
INGESTION_PACKAGES="$MONGO_PACKAGE,$KAFKA_PACKAGE"
FEEDER_PACKAGES="$KAFKA_PACKAGE,$CSV_PACKAGE"
ANALYSIS_PACKAGES="$MONGO_PACKAGE"

# Export variables for Python scripts
export KAFKA_BROKERS
export KAFKA_TOPIC
export CSV_PATH
export SPARK_HOME

# IMPORTANT: Clear any potentially conflicting Jupyter/PySpark environment variables
unset PYSPARK_PYTHON
unset PYSPARK_SUBMIT_ARGS
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS

echo "--- Starting Air Traffic Streaming Pipeline ---"

# ----------------------------------------------------------------------
# --- 1. Start Kafka Consumer (Kafka to MongoDB) ---
echo "Starting 1. StreamIngestionService (Writing to MongoDB) in the background..."

nohup bash -c "$SPARK_HOME/bin/spark-submit \
    --conf \"spark.pyspark.python=$PYTHON_EXECUTABLE\" \
    --packages $INGESTION_PACKAGES \
    StreamIngestionService.py" & 
CONSUMER_PID=$!
echo "Consumer PID: $CONSUMER_PID"
echo "Waiting 15 seconds for Consumer startup..."
sleep 15 

# ----------------------------------------------------------------------
# --- 2. Run Kafka Producer (CSV to Kafka) ---
echo "Starting 2. DataFeederService (CSV to Kafka)..."

$SPARK_HOME/bin/spark-submit \
    --conf "spark.pyspark.python=$PYTHON_EXECUTABLE" \
    --conf spark.hadoop.fs.defaultFS="file:///" \
    --packages $FEEDER_PACKAGES \
    DataFeederService.py

if [ $? -ne 0 ]; then
    echo "ERROR: Data Feeder Service failed. Halting pipeline."
    kill -9 $CONSUMER_PID 2>/dev/null || true
    exit 1
fi

echo "Waiting 20 seconds for MongoDB Consumer to ingest all data..."
sleep 20

# ----------------------------------------------------------------------
# --- 3. Run Batch Analysis (MongoDB to ML Insights) ---
echo "Starting 3. AnalyticsEngine.py (MongoDB to ML Insights)..."

# The Python script uses the full Java class name, matched here by --packages.
$SPARK_HOME/bin/spark-submit \
    --conf "spark.pyspark.python=$PYTHON_EXECUTABLE" \
    --packages $ANALYSIS_PACKAGES \
    AnalyticsEngine.py

ANALYSIS_EXIT_CODE=$?

# ----------------------------------------------------------------------
# --- 4. Cleanup ---
echo "Stopping StreamIngestionService (PID $CONSUMER_PID)..."
kill $CONSUMER_PID 2>/dev/null || true
pkill -f "python StreamIngestionService.py" 

if [ $ANALYSIS_EXIT_CODE -ne 0 ]; then
    echo "--- Pipeline Failed During Analysis ---"
    exit $ANALYSIS_EXIT_CODE
fi

echo "--- Pipeline Complete ---"