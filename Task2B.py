from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

### 2B1

# Create SparkSession
spark = SparkSession.builder \
    .appName("Task 2.B.1") \
    .getOrCreate()

# Load Customers and Purchases datasets into Spark DataFrames
customers_df = spark.read.csv("Customers.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("Purchases.csv", header=True, inferSchema=True)

# Join DataFrames based on 'ID' and 'CustID'
dataset = purchases_df.join(customers_df, purchases_df.CustID == customers_df.ID)

# Selecting required attributes for the dataset
dataset = dataset.select('CustID', 'TransID', 'Age', 'Salary', 'TransNumItems', 'TransTotal')

# Randomly split the dataset into Trainset (80%) and Testset (20%)
trainset, testset = dataset.randomSplit([0.8, 0.2], seed=123)



### 2B2

# Prepare features and target variable
assembler = VectorAssembler(
    inputCols=["Age", "Salary", "TransNumItems"],
    outputCol="features")

trainset = assembler.transform(trainset)
testset = assembler.transform(testset)

# Train Random Forest model
rf = RandomForestRegressor(featuresCol="features", labelCol="TransTotal", seed=123)
rf_model = rf.fit(trainset)

# Train Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="TransTotal")
lr_model = lr.fit(trainset)

# Make predictions on the testset
rf_predictions = rf_model.transform(testset)
lr_predictions = lr_model.transform(testset)


### 2B3

# Evaluate the models using MSE
evaluator_mse = RegressionEvaluator(labelCol="TransTotal", predictionCol="prediction", metricName="mse")

rf_mse = evaluator_mse.evaluate(rf_predictions)
lr_mse = evaluator_mse.evaluate(lr_predictions)

print("Random Forest Mean Squared Error (MSE):", rf_mse)
print("Linear Regression Mean Squared Error (MSE):", lr_mse)
print("Model performance is not very good because the data is randomly generated and no features have any relationship with target variable.")

# Stop SparkSession
spark.stop()



