from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = (SparkSession.builder
    .appName("Azure_Functions_KMeans_Queue")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-iis:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")

    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")

    .config("spark.executor.memory", "512m")
    .config("spark.driver.memory", "512m")
    .getOrCreate())

print("==== Connection to MinIO successful! ====")

path = "s3a://azure-functions-raw/azure-cleaned-queue-invocation.json"
df = spark.read.json(path)

print("==== Data Sample from MinIO: ====")
df.show(5)

vec_assembler = VectorAssembler(inputCols=["TotalDailyInvocations"], outputCol="features")
final_data = vec_assembler.transform(df)

kmeans = KMeans(featuresCol="features", k=3, seed=42)
model = kmeans.fit(final_data)

predictions = model.transform(final_data)

print("==== Classification Results (Sample): ====")
predictions.select("HashApp", "HashFunction", "TotalDailyInvocations", "prediction").show(10)

model.write().overwrite().save("s3a://azure-functions-raw/models/queue/azure_load_model")

try:
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print(f"==== Silhouette score = {silhouette} ====")
except Exception as e:
    print(f"==== Skipping Silhouette score (Too little data): {e} ====")

centers = model.clusterCenters()
print("==== Cluster Centers (Mean Calls): ====")
for i, center in enumerate(centers):
    print(f"Cluster {i}: {center[0]} calls per day")

centers_list = [(int(i), float(c[0])) for i, c in enumerate(centers)]
centers_df = spark.createDataFrame(centers_list, ["cluster_id", "centroid_invocations"])

centers_df.coalesce(1).write.mode("overwrite") \
    .csv("s3a://azure-functions-raw/results/queue/centroids.csv", header=True)

print("==== Centers saved successfully to MinIO! ====")

spark.stop()
