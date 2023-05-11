from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, minute, dayofweek, month
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from geopy import distance
from pyspark.sql.functions import when
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import count
import os

spark = SparkSession.builder \
    .appName("NYC Taxi Trip Duration") \
    .getOrCreate()

# Read train and test data
train = spark.read.csv("new_train.csv", header=True, inferSchema=True)

# Sample the data
train = train.sample(fraction=0.15, seed=1)

# Convert pickup_datetime to timestamp and create new columns
train = train.withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType())) \
    .withColumn("hour", hour("pickup_datetime")) \
    .withColumn("minute", minute("pickup_datetime")) \
    .withColumn("minute_oftheday", (col("hour") * 60 + col("minute"))) \
    .withColumn("day_week", dayofweek("pickup_datetime")) \
    .withColumn("month", month("pickup_datetime"))

# Drop columns from the train DataFrame
train = train.drop("pickup_datetime", "dropoff_datetime")

# Drop the id columns
train = train.drop("id")

# Define a UDF to calculate the distance
def get_distance(pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude):
    pick = (pickup_latitude, pickup_longitude)
    drop = (dropoff_latitude, dropoff_longitude)
    dist = distance.geodesic(pick, drop).km
    return dist

get_distance_udf = udf(get_distance, FloatType())

# Apply the UDF to the train and test DataFrames
train = train.withColumn("distance", get_distance_udf("pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude"))

# Replace 'N' and 'Y' with 0 and 1 in the store_and_fwd_flag column in both train and test DataFrames
train = train.withColumn("store_and_fwd_flag", when(col("store_and_fwd_flag") == "N", 0).otherwise(1))

# The value counts for the updated store_and_fwd_flag column in the train DataFrame
store_and_fwd_flag_counts_updated = train.groupBy("store_and_fwd_flag").agg(count("*").alias("count")).orderBy("store_and_fwd_flag")

# Remove the minute_oftheday column from the train and test DataFrames
train = train.drop("minute_oftheday")

# Define the label and features
label = "trip_duration"
features = [col for col in train.columns if col != label]

# Assemble the features into a single feature vector column
assembler = VectorAssembler(inputCols=features, outputCol="features")
train_with_features = assembler.transform(train)

# Rename the label column to "label" if it's not already named "label"
if label != "label":
    train_with_features = train_with_features.withColumnRenamed(label, "label")

from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline
import time
import os

# Create a directory to save the models
model_save_path = "best_models"
if not os.path.exists(model_save_path):
    os.makedirs(model_save_path)

# Prepare the validation data 90% train, 10% validation, seed 42
train_data, validation_data = train_with_features.randomSplit([0.9, 0.1], seed=42)

# Initialize the models
models = [
    ("Linear Regression", LinearRegression(maxIter=20)),
    ("Decision Tree Regressor", DecisionTreeRegressor(maxDepth=8)),
    ("Gradient Boosting Regressor", GBTRegressor(maxIter=20)),
]

# Set up the evaluation metrics
evaluator_mae = RegressionEvaluator(metricName="mae")
evaluator_r2 = RegressionEvaluator(metricName="r2")

# Train and evaluate the models
results = []
for name, model in models:
    start_time = time.time()
    
    # Create a pipeline with the model
    pipeline = Pipeline(stages=[model])

    # Train the model using cross-validation
    param_grid = ParamGridBuilder().build()
    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_mae,
        numFolds=3
    )
    cv_model = cross_validator.fit(train_with_features)

    # Make predictions on the validation data
    predictions = cv_model.transform(validation_data)

    # Evaluate the model
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    duration = time.time() - start_time

    results.append((name, mae, r2, duration))

    # Save the best model
    best_model = cv_model.bestModel
    best_model.save(os.path.join(model_save_path, name.replace(" ", "_")))

# Create a comparison table
comparison_table = spark.createDataFrame(results, ["Model", "MAE", "R2 Score", "Duration"])
comparison_table.show(truncate=False)