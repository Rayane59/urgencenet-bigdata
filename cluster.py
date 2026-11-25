from pathlib import Path
from time import monotonic, sleep
from urllib.request import urlopen
import json

from pyspark.sql import SparkSession


def start_spark(local=False):
    builder = SparkSession.builder.appName("UrgenceNet - Jalon 1")
    builder = builder.config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
    if local:
        builder = builder.master("local[4]")
    else:
        deadline = monotonic() + 90
        while True:
            try:
                with urlopen("http://spark-master:8080/json/", timeout=3) as response:
                    active = [w for w in json.load(response)["workers"] if w["state"] == "ALIVE"]
                if len(active) == 2:
                    break
            except OSError:
                pass
            if monotonic() >= deadline:
                raise RuntimeError("Les deux Workers Spark ne sont pas actifs.")
            sleep(2)

        Path("/project/spark-events").mkdir(exist_ok=True)
        settings = {
            "spark.driver.host": "jupyter",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.cores.max": "4",
            "spark.executor.cores": "2",
            "spark.executor.memory": "768m",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "file:///project/spark-events",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.access.key": "urgencenet",
            "spark.hadoop.fs.s3a.secret.key": "urgency-demo-local",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }
        builder = builder.master("spark://spark-master:7077")
        for key, value in settings.items():
            builder = builder.config(key, value)
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print(f"Spark={spark.version} | master={sc.master} | appName={sc.appName} | sc.defaultParallelism={sc.defaultParallelism}")
    return spark


if __name__ == "__main__":
    session = start_spark()
    try:
        assert session.sparkContext.parallelize(range(4), 4).sum() == 6
    finally:
        session.stop()
