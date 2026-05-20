#!/bin/bash
# Install packages to shared volume on every startup
python3 -m pip install redis --target=/opt/spark-apps/packages --quiet 2>/dev/null || true

# Start the appropriate Spark role based on SPARK_ROLE env var
if [ "$SPARK_ROLE" = "worker" ]; then
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
else
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
fi