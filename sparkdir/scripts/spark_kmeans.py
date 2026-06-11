from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# 1. Initialize Spark with S3/MinIO configurations
spark = (SparkSession.builder
    .appName("Azure_Functions_KMeans")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-iis:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # ΡΗΤΕΣ ΚΑΘΑΡΕΣ ΤΙΜΕΣ ΓΙΑ ΤΑ TIMEOUTS ("60s" και "30s")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
    
    # ΚΡΙΣΙΜΗ ΔΙΟΡΘΩΣΗ ΓΙΑ ΤΟ ΣΦΑΛΜΑ "24h":
    # Μετατρέπουμε το 24h σε καθαρό αριθμό δευτερολέπτων (24 * 3600 = 86400)
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    
    .config("spark.executor.memory", "512m")
    .config("spark.driver.memory", "512m")
    .getOrCreate())

print("==== Σύνδεση με το MinIO επιτυχής! ====")

# 2. Φορτώνουμε το JSON αρχείο των Azure Functions από το σωστό Bucket
path = "s3a://azure-functions-raw/azure-cleaned-invocation.json"
df = spark.read.json(path)

print("==== Δείγμα Δεδομένων από το MinIO: ====")
df.show(5)

# 3. Χρησιμοποιούμε τη στήλη 'TotalDailyInvocations' ως χαρακτηριστικό (Feature)
vec_assembler = VectorAssembler(inputCols=["TotalDailyInvocations"], outputCol="features")
final_data = vec_assembler.transform(df)

# 4. Train the KMeans Model (k=3 για Χαμηλό, Μεσαίο, Υψηλό φορτίο)
kmeans = KMeans(featuresCol="features", k=3, seed=42)
model = kmeans.fit(final_data)

# 5. Make Predictions
predictions = model.transform(final_data)

# 6. Εμφάνιση αποτελεσμάτων με βάση τα δικά σου πεδία
print("==== Αποτελέσματα Κατηγοριοποίησης (Δείγμα): ====")
predictions.select("HashApp", "HashFunction", "TotalDailyInvocations", "prediction").show(10)

# Αποθήκευση του μοντέλου στο σωστό bucket
model.write().overwrite().save("s3a://azure-functions-raw/models/azure_load_model")

# 7. Evaluate clustering by computing Silhouette score (Με προστασία try-except)
try:
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print(f"==== Silhouette score = {silhouette} ====")
except Exception as e:
    print(f"==== Παράκαμψη Silhouette score (Πολύ λίγα δεδομένα): {e} ====")

# 8. Εμφάνιση των κέντρων (centroids)
centers = model.clusterCenters()
print("==== Κέντρα Clusters (Μέσοι Όροι Κλήσεων): ====")
for i, center in enumerate(centers):
    print(f"Cluster {i}: {center[0]} κλήσεις τη μέρα")

# 9. Αποθήκευση των Κέντρων στο MinIO για να τα διαβάσει μετά το Node-RED
centers_list = [(int(i), float(c[0])) for i, c in enumerate(centers)]
centers_df = spark.createDataFrame(centers_list, ["cluster_id", "centroid_invocations"])

centers_df.write.mode("overwrite") \
    .csv("s3a://azure-functions-raw/results/centroids.csv", header=True)

print("==== Τα κέντρα αποθηκεύτηκαν με επιτυχία στο MinIO! ====")

spark.stop()