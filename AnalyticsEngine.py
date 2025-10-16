import os
import sys
import subprocess
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier
)
from pyspark.ml.regression import (
    DecisionTreeRegressor
)
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.sql.functions import when, col, lit, sum as spark_sum, coalesce, log
import pandas as pd
from collections import Counter 

# --- Critical Dependency Check and Installation ---
try:
    import tabulate
except ImportError:
    print("Missing 'tabulate' dependency. Attempting automatic installation...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "tabulate"])
        print("'tabulate' installed successfully.")
    except Exception as e:
        print(f"Error installing 'tabulate': {e}")
        print("Please manually ensure 'tabulate' is installed in your Spark environment.")
        sys.exit(1)


# --- Metric Calculation Helpers (Same as before) ---
# ... (Metric functions remain unchanged for brevity) ...
def safe_divide(numerator, denominator):
    return numerator / denominator if denominator != 0 and denominator is not None else 0

def calculate_metrics(TP, FP, FN, TN):
    precision_1 = safe_divide(TP, TP + FP)
    recall_1 = safe_divide(TP, TP + FN)
    f1_score_1 = safe_divide(2 * precision_1 * recall_1, precision_1 + recall_1)
    
    precision_0 = safe_divide(TN, TN + FN)
    recall_0 = safe_divide(TN, TN + FP)
    f1_score_0 = safe_divide(2 * precision_0 * recall_0, precision_0 + recall_0)
    
    support_0 = TN + FP
    support_1 = TP + FN

    return {
        '0': {'precision': precision_0, 'recall': recall_0, 'f1-score': f1_score_0, 'support': support_0},
        '1': {'precision': precision_1, 'recall': recall_1, 'f1-score': f1_score_1, 'support': support_1},
        'total_support': support_0 + support_1
    }

def print_prediction_results(model, test_df, model_name, label_col="is_delayed"):
    print(f"\n--- Results for {model_name} ---")

    predictions = model.transform(test_df)
    total_samples = test_df.count()

    if label_col == "is_delayed":
        evaluator = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print(f"Model Accuracy: {accuracy:.4f}")
        
        prediction_label_pairs = predictions.select(label_col, "prediction").rdd.map(
            lambda row: (int(row[label_col]), int(row["prediction"]))
        )
        
        counts = prediction_label_pairs.countByValue()
        
        TN = counts.get((0, 0), 0) 
        FP = counts.get((0, 1), 0) 
        FN = counts.get((1, 0), 0) 
        TP = counts.get((1, 1), 0) 
        
        print("\n[CONFUSION MATRIX]")
        cm_data = {
            "Actual": ["Not Delayed (0)", "Delayed (1)"],
            "Predicted Not Delayed (0)": [TN, FN],
            "Predicted Delayed (1)": [FP, TP]
        }
        cm_df = pd.DataFrame(cm_data).set_index("Actual")
        print(cm_df.to_markdown())

        print("\nDetailed Classification Report:")
        metrics = calculate_metrics(TP, FP, FN, TN)
        
        report_data = {
            'precision': [metrics['0']['precision'], metrics['1']['precision']],
            'recall': [metrics['0']['recall'], metrics['1']['recall']],
            'f1-score': [metrics['0']['f1-score'], metrics['1']['f1-score']],
            'support': [metrics['0']['support'], metrics['1']['support']],
        }
        report_df = pd.DataFrame(report_data, index=['0', '1'])
        
        weighted_avg = report_df.mul(report_df['support'], axis=0).sum() / report_df['support'].sum()
        macro_avg = report_df.drop(columns=['support']).mean()

        report_df.loc['accuracy'] = [0, 0, accuracy, total_samples]
        report_df.loc['macro avg'] = [macro_avg['precision'], macro_avg['recall'], macro_avg['f1-score'], total_samples]
        report_df.loc['weighted avg'] = [weighted_avg['precision'], weighted_avg['recall'], weighted_avg['f1-score'], total_samples]

        print_df = report_df.applymap(lambda x: f"{x:.2f}" if isinstance(x, (float, int)) and x <= 1.0 else f"{int(x)}").fillna('')
        print(print_df.to_markdown())

        print("\n[RESULTS SAMPLE]")
        
        result_df = predictions.select(
            col(label_col).alias("Actual Delay (1=True)"), 
            col("prediction").alias("Predicted Delay"), 
            (col(label_col) == col("prediction")).alias("Correct Prediction"),
            "origin_airport",
            "carrier_flight_id"
        )
        
        pd_df = result_df.limit(10).toPandas()
        print(pd_df.to_markdown(index=False))

    else: # delay_minutes_target
        evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print(f"RMSE: {rmse:.4f}")
# ... (End of Metric functions) ...


# # 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("AirTrafficOptimizationAnalysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# --- FILE PATH ---
input_file = "airlines_extended_15000.csv"
csv_path = f"file:///home/talentum/shared/{input_file}" 
print(f"Reading accumulated data from the provided file: {input_file}")
print(f"Attempting to read CSV from specific path: {csv_path}")

try:
    historical_df = spark.read.csv(csv_path, header=True, inferSchema=True)
except Exception as e:
    fallback_path = f"file:///home/talentum/{input_file}"
    print(f"Reading from shared folder failed. Trying home directory path: {fallback_path}")
    try:
        historical_df = spark.read.csv(fallback_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"\nFATAL ERROR: Could not read CSV file. Please ensure '{input_file}' is at either '/home/talentum/shared/' or '/home/talentum/'.")
        spark.stop()
        sys.exit(1)


if historical_df.count() < 1000:
    print(f"WARNING: Data count is low ({historical_df.count()}). Check data integrity.")
    spark.stop()
    sys.exit(1)

# # 2. Data Preparation and Feature Engineering
print("\n--- Data Preprocessing Steps ---")

# Calculate new feature: Average Delay (Total Minutes Delayed / Total Flights)
# Use coalesce and lit(1) to prevent division by zero, assuming at least one flight if minutes are present.
total_flights = col("`Statistics.Flights.Total`")
total_minutes = col("`Statistics.Minutes Delayed.Total`")

# Calculate Average Delay
avg_delay = total_minutes / coalesce(total_flights, lit(1))

# Calculate Cancellation Ratio
cancel_ratio = col("`Statistics.Flights.Cancelled`") / coalesce(total_flights, lit(1))

# Calculate NAS Delay Ratio (handle potential division by zero for total minutes)
total_delay_minutes = coalesce(total_minutes, lit(0)).cast("double")
nas_delay_ratio = col("`Statistics.Minutes Delayed.National Aviation System`") / when(total_delay_minutes > 0, total_delay_minutes).otherwise(lit(1))

# Calculate Carrier Delay Ratio (handle potential division by zero for total minutes)
carrier_delay_ratio = col("`Statistics.Minutes Delayed.Carrier`") / when(total_delay_minutes > 0, total_delay_minutes).otherwise(lit(1))


# **STEP 1: Feature Creation and Label Correction**
df = historical_df.select(
    # Categorical Feature: Airport code
    col("`Airport.Code`").alias("origin_airport"), 
    # Categorical Feature: Carrier name(s) (used as a combined carrier/flight ID)
    col("`Statistics.Carriers.Names`").alias("carrier_flight_id"),
    
    # **FIXED CLASSIFICATION LABEL (is_delayed)**: Average delay > 15 minutes
    when(avg_delay > 15.0, 1).otherwise(0).alias("is_delayed"), 
    
    # **REGRESSION TARGET**: Actual total delay in minutes
    total_minutes.cast("double").alias("delay_minutes_target"), 
    
    # **NEW QUANTITATIVE FEATURES**
    avg_delay.alias("avg_delay_per_flight"),
    cancel_ratio.alias("cancellation_ratio"),
    nas_delay_ratio.alias("nas_delay_ratio"),
    carrier_delay_ratio.alias("carrier_delay_ratio"),
    col("`Time.Month`").cast("double").alias("month_feature")
)
print(f"1. Initial Features Extracted: {df.columns}")

# --- Data Cleaning and Feature Processing ---
df_clean = df.na.drop()
print(f"2. Data Cleaning: Rows with nulls in key columns dropped. Remaining count: {df_clean.count()}")


# **STEP 2: Indexing and Assembling**
# 1. Indexers: Convert categorical strings to numerical indices. 
originIndexer = StringIndexer(inputCol="origin_airport", outputCol="origin_index", handleInvalid="skip")
carrierIndexer = StringIndexer(inputCol="carrier_flight_id", outputCol="carrier_index", handleInvalid="skip")
print("3. StringIndexer: Categorical features (origin_airport, carrier_flight_id) are converted to indices.")

# 2. Assembler: Combine the new and indexed features
feature_cols = [
    "origin_index", 
    "carrier_index",
    "avg_delay_per_flight",
    "cancellation_ratio",
    "nas_delay_ratio",
    "carrier_delay_ratio",
    "month_feature"
]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
print(f"4. VectorAssembler: Features combined into a single vector column ('raw_features') using inputs: {feature_cols}")

# 3. Standardizer: Crucial for Logistic Regression/other linear models
# StandardScaler improves the performance of many ML algorithms by standardizing the features.
scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                        withStd=True, withMean=False)
print("5. StandardScaler: Raw features standardized (output column 'features').")


# Split the cleaned data
(trainingData, testData) = df_clean.randomSplit([0.7, 0.3], seed=42)

# --- Print Sample Counts ---
training_count = trainingData.count()
testing_count = testData.count()
print(f"\n--- Sample Split Counts ---")
print(f"Total Clean Samples: {training_count + testing_count}")
print(f"Training Samples (70%): {training_count}")
print(f"Testing Samples (30%): {testing_count}")


if trainingData.count() == 0:
    print("\nFATAL ERROR: Training dataset is EMPTY after cleaning. Check data quality.")
    spark.stop()
    sys.exit(1)

# Define the base stages for the pipeline (updated to include StandardScaler)
base_stages = [originIndexer, carrierIndexer, assembler, scaler]

print("\nStarting model training and evaluation with enhanced features and corrected label...")

# # 3. Model Training and Evaluation

# --- Model 1: Logistic Regression (Classification) ---
lr = LogisticRegression(labelCol="is_delayed", featuresCol="features", maxIter=10)
lr_pipeline = Pipeline(stages=base_stages + [lr])
lr_model = lr_pipeline.fit(trainingData)
print_prediction_results(lr_model, testData, "Logistic Regression (Enhanced)", "is_delayed")


# --- Model 2: Random Forest Classifier (Classification) ---
rf = RandomForestClassifier(labelCol="is_delayed", featuresCol="features", numTrees=30, seed=42) # Increased trees
rf_pipeline = Pipeline(stages=base_stages + [rf])
rf_model = rf_pipeline.fit(trainingData)
print_prediction_results(rf_model, testData, "Random Forest Classifier (Enhanced)", "is_delayed")


# --- Model 3: Decision Tree Regressor (Regression) ---
# Note: Regression model performance will improve but RMSE will still be large due to large delay numbers.
dt_reg = DecisionTreeRegressor(labelCol="delay_minutes_target", featuresCol="features", maxDepth=8) # Increased depth
dt_pipeline = Pipeline(stages=base_stages + [dt_reg])
dt_model = dt_pipeline.fit(trainingData)
print_prediction_results(dt_model, testData, "Decision Tree Regressor (Enhanced)", "delay_minutes_target")


print(f"\n--- Analysis Complete ---")
spark.stop()