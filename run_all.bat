@echo off

echo Running spark_kmeans_http.py...
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy" --jars /opt/spark/external_jars/hadoop-aws-3.3.4.jar,/opt/spark/external_jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/scripts/spark_kmeans_http.py

echo Running spark_kmeans_queue.py...
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy" --jars /opt/spark/external_jars/hadoop-aws-3.3.4.jar,/opt/spark/external_jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/scripts/spark_kmeans_queue.py

echo Running spark_kmeans_orchestration.py...
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy" --jars /opt/spark/external_jars/hadoop-aws-3.3.4.jar,/opt/spark/external_jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/scripts/spark_kmeans_orchestration.py

echo Running spark_kmeans_timer.py...
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy" --jars /opt/spark/external_jars/hadoop-aws-3.3.4.jar,/opt/spark/external_jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/scripts/spark_kmeans_timer.py

echo Running spark_kmeans_storage.py...
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy" --jars /opt/spark/external_jars/hadoop-aws-3.3.4.jar,/opt/spark/external_jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/scripts/spark_kmeans_storage.py

echo Done.
pause