import argparse
import json
from statistics import median
from time import perf_counter

from pyspark.sql import functions as F, types as T

from cluster import start_spark

EVENT = T.StructType([
    T.StructField(name, datatype()) for name, datatype in [
        ("visit_id", T.StringType), ("patient_id", T.StringType), ("facility_id", T.StringType),
        ("city", T.StringType), ("status", T.StringType), ("priority", T.IntegerType),
        ("wait_minutes", T.IntegerType), ("department", T.StringType), ("desk_id", T.StringType),
        ("transfer_to", T.StringType), ("event_time", T.TimestampType),
    ]
])
FACILITY = T.StructType([
    T.StructField(name, datatype()) for name, datatype in [
        ("facility_id", T.StringType), ("name", T.StringType), ("city", T.StringType),
        ("hospital_group", T.StringType), ("type", T.StringType), ("beds", T.IntegerType),
        ("latitude", T.DoubleType), ("longitude", T.DoubleType),
    ]
])


def wait_band(minutes):
    if minutes is None:
        return None
    if minutes < 15:
        return "court"
    return "moyen" if minutes <= 45 else "long"


def sum_count(left, right):
    return left[0] + right[0], left[1] + right[1]


def benchmark(sc, operations, repeats=3):
    samples = {label: [] for label in operations}
    results = {}
    for index in range(repeats + 1):
        for label in operations if index % 2 == 0 else reversed(operations):
            sc.setJobDescription(label)
            start = perf_counter()
            results[label] = operations[label]()
            elapsed = perf_counter() - start
            if index:
                samples[label].append(elapsed)
    for label, times in samples.items():
        print(f"{label}: médiane {median(times):.3f} s ; essais {[round(t, 3) for t in times]}")
    return results


def run(spark, history, facilities, silver):
    sc = spark.sparkContext
    sc.setJobGroup("RDD", "Partie 2 - Spark Core")
    events_rdd = sc.textFile(history, minPartitions=4).map(json.loads)
    care_rdd = events_rdd.filter(lambda event: event["status"] == "CARE_STARTED" and event["wait_minutes"] is not None)
    counts = care_rdd.map(lambda event: (event["city"], 1)).reduceByKey(lambda a, b: a + b, 4)
    def wait_stats():
        return care_rdd.map(lambda event: (event['city'], (event['wait_minutes'], 1))).reduceByKey(sum_count, 4)
    priorities = (events_rdd.filter(lambda event: event["status"] == "TRIAGED")
                  .map(lambda event: (event["priority"], 1)).reduceByKey(lambda a, b: a + b, 4))
    facilities_rdd = care_rdd.map(lambda event: (event["facility_id"], 1)).reduceByKey(lambda a, b: a + b, 4)
    print("Ville | Prises en charge (brut)")
    print(sorted(counts.collect()))
    print("Priorité | Passages triés (brut)")
    print(sorted(priorities.collect()))
    print("Top 5 établissements (brut)")
    print(facilities_rdd.takeOrdered(5, key=lambda pair: (-pair[1], pair[0])))
    print("Lineage RDD (délai par ville)")
    print(wait_stats().toDebugString().decode("utf-8"))

    sc.setJobGroup("SQL", "Partie 3 - Spark SQL")
    events = spark.read.schema(EVENT).option("mode", "FAILFAST").json(history)
    reference = spark.read.schema(FACILITY).option("mode", "FAILFAST").json(facilities)
    care = events.where((F.col("status") == "CARE_STARTED") & F.col("wait_minutes").isNotNull())
    by_city = care.groupBy("city").agg(F.count("*").alias("nombre"), F.avg("wait_minutes").alias("delai_moyen"))
    events.createOrReplaceTempView("urgences")
    by_sql = spark.sql("""
        SELECT city, count(*) AS nombre, avg(wait_minutes) AS delai_moyen
        FROM urgences WHERE status = 'CARE_STARTED' AND wait_minutes IS NOT NULL
        GROUP BY city
    """)
    sc.setJobGroup("BENCH", "Comparaison RDD et DataFrame")
    measured = benchmark(sc, {"RDD délai par ville": lambda: wait_stats().collect(), "DataFrame délai par ville": by_city.collect})
    rdd_by_city = {city: (count, total / count) for city, (total, count) in measured["RDD délai par ville"]}
    df_by_city = {row.city: (row.nombre, row.delai_moyen) for row in measured["DataFrame délai par ville"]}
    sql_by_city = {row.city: (row.nombre, row.delai_moyen) for row in by_sql.collect()}
    assert rdd_by_city == df_by_city == sql_by_city
    print("Ville | Nombre | Délai moyen (min) — mêmes résultats RDD, DataFrame et SQL")
    for city, (count, average) in sorted(rdd_by_city.items()):
        print(f"{city:12} | {count:5} | {average:.4f}")

    sc.setJobGroup("SQL", "Partie 3 - Jointure, qualité et Silver")
    enriched = care.join(F.broadcast(reference.select("facility_id", "type")), "facility_id", "inner")
    enriched.groupBy("city", "type").agg(
        F.count("*").alias("nombre"), F.round(F.avg("wait_minutes"), 4).alias("delai_moyen")
    ).orderBy("city", "type").show(40, truncate=False)
    print("EXPLAIN EXTENDED — jointure établissements")
    enriched.explain(True)

    band_udf = F.udf(wait_band, T.StringType())
    band_expr = (
        F.when(F.col("wait_minutes").isNull(), F.lit(None).cast("string"))
         .when(F.col("wait_minutes") < 15, "court")
         .when(F.col("wait_minutes") <= 45, "moyen")
         .otherwise("long")
    )
    bands = events.select(band_udf("wait_minutes").alias("udf"), band_expr.alias("expression"))
    assert bands.where(~F.col("udf").eqNullSafe(F.col("expression"))).count() == 0
    bands.groupBy("expression").count().orderBy("expression").show()

    before = events.count()
    clean = events.dropDuplicates(["visit_id", "status"])
    after = clean.count()
    arrivals = clean.where(F.col("status") == "ARRIVED").select(
        "visit_id", F.col("event_time").alias("arrival_time")
    )
    starts = clean.where(F.col("status") == "CARE_STARTED").select(
        "visit_id", F.col("event_time").alias("care_time"), "wait_minutes"
    )
    paired = starts.join(arrivals, "visit_id", "left").withColumn(
        "recalculated_minutes", F.floor((F.unix_timestamp("care_time") - F.unix_timestamp("arrival_time")) / 60)
    )
    difference = F.col("arrival_time").isNotNull() & ~F.col("recalculated_minutes").eqNullSafe(F.col("wait_minutes"))
    minutes = F.col("recalculated_minutes")
    as_hmm = F.floor(minutes / 60) * 100 + F.pmod(minutes, F.lit(60))
    check = paired.agg(
        F.count("*").alias("prises_en_charge"),
        F.count("arrival_time").alias("arrivees_retrouvees"),
        F.count(F.when(difference, 1)).alias("ecarts"),
        F.count(F.when(difference & (minutes >= 60) & (F.col("wait_minutes") == as_hmm), 1)).alias("ecarts_hmm"),
    ).first()
    print(f"Auto-jointure: {check.prises_en_charge} prises en charge, {check.arrivees_retrouvees} arrivées retrouvées, {check.ecarts} écarts")
    print(f"Contrôle des délais: {check.prises_en_charge - check.arrivees_retrouvees} sans arrivée, {check.ecarts_hmm} écarts au format hmm")
    print(f"Dédoublonnage: {before} avant, {after} après, {before - after} supprimés")
    clean.repartition("city").write.mode("overwrite").partitionBy("city").parquet(silver)
    readback = spark.read.parquet(silver)
    print(f"Parquet Silver relu depuis {silver}")
    readback.groupBy("city").count().orderBy("city").show()
    assert readback.count() == after


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Diagnostic hors Docker")
    options = parser.parse_args()
    spark = start_spark(options.local)
    try:
        source = "data/urgences_history.jsonl" if options.local else "s3a://lake/bronze/history/urgences_history.jsonl"
        reference = "data/facilities.json" if options.local else "s3a://lake/bronze/history/facilities.json"
        target = "data/silver_test" if options.local else "s3a://lake/silver/urgences_history/"
        run(spark, source, reference, target)
    finally:
        spark.stop()
