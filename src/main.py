import sys
import glob
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

# Add custom profiler path
sys.path.append("/workspace/project")
from spark_profiler import SparkProfiler


def main():
    # 1. Setup Command Line Arguments
    parser = argparse.ArgumentParser(description="Taxi Data Pipeline")
    parser.add_argument("--input_path", required=True, help="Path to parquet files (e.g., /data/project/*.parquet)")
    parser.add_argument("--zone_path", required=True, help="Path to taxi_zone_lookup.csv")
    parser.add_argument("--scale", type=float, default=1.0, help="Fraction of dataset to process (0.1 to 1.0)")
    args = parser.parse_args()

    # 2. Initialize Spark
    master = "spark://spark-master:7077"
    spark = (SparkSession.builder
             .master(master)
             .appName("project")
             .config("spark.ui.showConsoleProgress", "false")
             # AQE optimizations left on globally
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.adaptive.skewJoin.enabled", "true")
             .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
             .config("spark.sql.shuffle.partitions", "32")
             .getOrCreate())

    print("Spark UI:", spark.sparkContext.uiWebUrl)
    profiler = SparkProfiler(spark)

    # 3. Load Zone Lookup Broadcast (Used across multiple stages)
    zone_lookup_full = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(args.zone_path)
    .select(
        sf.col("LocationID").cast("integer").alias("zone_id"),
        sf.col("Borough").alias("borough"),
        sf.col("Zone").alias("zone_name")
    ))

    zone_lookup = zone_lookup_full.select(
        sf.col("zone_id"),
        sf.col("borough").alias("pickup_borough")
    )

    # ==========================================
    # M1: Data Cleaning
    # ==========================================
    print("\nStarting M1...")
    raw_trips = spark.read.parquet(args.input_path)

    trips = (raw_trips
             .withColumn("tpep_pickup_datetime", sf.col("tpep_pickup_datetime").cast("timestamp"))
             .withColumn("tpep_dropoff_datetime", sf.col("tpep_dropoff_datetime").cast("timestamp"))
             .withColumn("PULocationID", sf.col("PULocationID").cast("integer"))
             .withColumn("DOLocationID", sf.col("DOLocationID").cast("integer"))
             .withColumn("passenger_count", sf.col("passenger_count").cast("integer"))
             .withColumn("trip_distance", sf.col("trip_distance").cast("double"))
             .withColumn("fare_amount", sf.col("fare_amount").cast("double"))
             .withColumn("year", sf.year(sf.col("tpep_pickup_datetime")))
             )

    invalid_fare = (sf.col("fare_amount").isNull()) | (sf.col("fare_amount") <= 0)
    invalid_distance = (sf.col("trip_distance").isNull()) | (sf.col("trip_distance") <= 0)
    invalid_passengers = (sf.col("passenger_count").isNull()) | (sf.col("passenger_count") == 0) | (
                sf.col("passenger_count") > 6)
    invalid_timestamps = (sf.col("tpep_pickup_datetime").isNull()) | (sf.col("tpep_dropoff_datetime").isNull()) | (
                sf.col("tpep_dropoff_datetime") <= sf.col("tpep_pickup_datetime"))
    invalid_locations = (sf.col("PULocationID").isNull()) | (sf.col("DOLocationID").isNull()) | \
                        (~sf.col("PULocationID").between(1, 263)) | (~sf.col("DOLocationID").between(1, 263))

    invalid_any = invalid_fare | invalid_distance | invalid_passengers | invalid_timestamps | invalid_locations

    cleaning_stats = (trips
                      .groupBy("year")
                      .agg(
        sf.count("*").alias("total_records_processed"),
        sf.sum(invalid_fare.cast("integer")).alias("removed_invalid_fare"),
        sf.sum(invalid_distance.cast("integer")).alias("removed_invalid_distance"),
        sf.sum(invalid_passengers.cast("integer")).alias("removed_invalid_passengers"),
        sf.sum(invalid_timestamps.cast("integer")).alias("removed_invalid_timestamps"),
        sf.sum(invalid_locations.cast("integer")).alias("removed_invalid_locations"),
        sf.sum(invalid_any.cast("integer")).alias("total_records_removed")
    )
                      .orderBy("year")
                      )

    print("--- M1 Data Cleaning Statistics Report ---")
    cleaning_stats.show(truncate=False)

    # Apply scaling for scalability tests
    m1_clean_trips = trips.filter(~invalid_any).sample(fraction=args.scale, seed=42)

    # ==========================================
    # M2: Temporal Demand Analysis
    # ==========================================
    print("\nStarting M2...")
    spark.sparkContext.setJobGroup("M2_ANALYSIS", "M2: Temporal Demand")

    m2_prepared_trips = (m1_clean_trips
                         .withColumn("day_of_week", sf.dayofweek(sf.col("tpep_pickup_datetime")))
                         .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
                         .withColumn("duration_minutes",
                                     (sf.unix_timestamp(sf.col("tpep_dropoff_datetime")) - sf.unix_timestamp(
                                         sf.col("tpep_pickup_datetime"))) / 60.0
                                     )
                         )

    m2_demand_analysis = (m2_prepared_trips
                          .join(sf.broadcast(zone_lookup), m2_prepared_trips.PULocationID == zone_lookup.zone_id,
                                how="inner")
                          .groupBy("pickup_borough", "day_of_week", "hour_of_day")
                          .agg(
        sf.count("*").alias("total_trip_count"),
        sf.mean("fare_amount").alias("mean_fare_amount"),
        sf.mean("duration_minutes").alias("mean_trip_duration_min"),
        sf.mean("trip_distance").alias("mean_trip_distance")
    )
                          .orderBy("pickup_borough", "day_of_week", "hour_of_day")
                          )

    output_path_m2 = "/tmp/m2_temporal_demand_output"
    (m2_demand_analysis.write.mode("overwrite").partitionBy("pickup_borough").parquet(output_path_m2))

    print("--- M2 Temporal Demand Analysis 10 first ---")
    m2_demand_analysis.show(10, truncate=False)

    # ==========================================
    # M3: Zone-Level Statistics
    # ==========================================
    print("\nStarting M3...")
    spark.sparkContext.setJobGroup("M3_STATS", "M3: Zone-Level Statistics")

    m3_prepared = (m1_clean_trips
                   .withColumn("year_month", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyy-MM"))
                   .join(sf.broadcast(zone_lookup), m1_clean_trips.PULocationID == zone_lookup.zone_id, how="inner")
                   .filter(sf.col("pickup_borough").isNotNull())
                   )

    m3_monthly_zone_stats = (m3_prepared
    .groupBy("pickup_borough", "PULocationID", "year_month")
    .agg(
        sf.count("*").alias("monthly_trip_count"),
        sf.mean("fare_amount").alias("mean_fare_amount"),
        sf.stddev("fare_amount").alias("std_fare_amount"),
        sf.mean("trip_distance").alias("mean_trip_distance"),
        sf.stddev("trip_distance").alias("std_trip_distance")
    )
    )

    w_borough_monthly_rank = Window.partitionBy("pickup_borough", "year_month").orderBy(
        sf.col("monthly_trip_count").desc())
    w_borough_monthly_total = Window.partitionBy("pickup_borough", "year_month")
    w_zone_rolling_3m = Window.partitionBy("PULocationID").orderBy("year_month").rowsBetween(-2, 0)

    m3_final = (m3_monthly_zone_stats
    .withColumn("zone_volume_rank", sf.rank().over(w_borough_monthly_rank))
    .withColumn("borough_monthly_trips", sf.sum("monthly_trip_count").over(w_borough_monthly_total))

    # Fixed Rolling Math: Sum the raw trips over 3 months, then divide
    .withColumn("rolling_3m_zone_trips", sf.sum("monthly_trip_count").over(w_zone_rolling_3m))
    .withColumn("rolling_3m_borough_trips", sf.sum("borough_monthly_trips").over(w_zone_rolling_3m))
    .withColumn("rolling_3m_trip_share", sf.col("rolling_3m_zone_trips") / sf.col("rolling_3m_borough_trips"))

    .withColumn("rolling_volume_mean", sf.avg("monthly_trip_count").over(w_zone_rolling_3m))
    .withColumn("rolling_volume_std", sf.stddev("monthly_trip_count").over(w_zone_rolling_3m))

    .withColumn(
        "is_volume_anomaly",
        sf.when(
            sf.col("rolling_volume_std").isNotNull() & (sf.col("rolling_volume_std") > 0),
            sf.abs(sf.col("monthly_trip_count") - sf.col("rolling_volume_mean")) > (1.5 * sf.col("rolling_volume_std"))
        ).otherwise(False)
    )
    )

    output_path_m3 = "/tmp/m3_zone_window_stats_output"
    (m3_final.write.mode("overwrite").partitionBy("pickup_borough").parquet(output_path_m3))

    print("--- M3 Zone-Level Window Statistics (First 3 Rows) ---")
    m3_final.show(3, vertical=True, truncate=False)

    # ==========================================
    # O1: Fare Anomaly Detection (Baseline & Optimized)
    # ==========================================
    print("\nStarting O1 Baseline...")
    spark.sparkContext.setJobGroup("O1_ANOMALY_BASELINE", "O1: Baseline")

    with profiler.profile("O1-fare-anomaly-baseline"):
        o1_base = (m1_clean_trips
                   .filter(sf.col("trip_distance") > 0)
                   .withColumn("fare_per_dist", sf.col("fare_amount") / sf.col("trip_distance"))
                   .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
                   .withColumn("is_weekend", sf.dayofweek(sf.col("tpep_pickup_datetime")).isin([1, 7]))
                   )

        o1_stats = (o1_base
        .groupBy("PULocationID", "hour_of_day")
        .agg(
            sf.mean("fare_per_dist").alias("mean_fare_per_dist"),
            sf.stddev("fare_per_dist").alias("std_fare_per_dist")
        )
        )

        o1_flagged = (o1_base
        .join(o1_stats, on=["PULocationID", "hour_of_day"], how="inner")
        .join(zone_lookup, on="PULocationID", how="inner")
        .withColumn(
            "is_fare_anomaly",
            sf.when(
                sf.col("std_fare_per_dist").isNotNull() & (sf.col("std_fare_per_dist") > 0),
                sf.col("fare_per_dist") > (sf.col("mean_fare_per_dist") + (3 * sf.col("std_fare_per_dist")))
            ).otherwise(False)
        )
        )

        o1_summary = (o1_flagged
                      .groupBy("pickup_borough", "payment_type", "is_weekend")
                      .agg(
            sf.count("*").alias("total_trips"),
            sf.sum(sf.when(sf.col("is_fare_anomaly"), 1).otherwise(0)).alias("anomaly_trips"),
            (sf.sum(sf.when(sf.col("is_fare_anomaly"), 1).otherwise(0)) / sf.count("*")).alias("anomaly_rate")
        )
                      .orderBy("pickup_borough", "payment_type", "is_weekend")
                      )
        o1_summary.show(10, truncate=False)

    print("\nStarting O1 Optimized...")
    spark.sparkContext.setJobGroup("O1_ANOMALY_OPTIMIZED", "O1: Optimized")

    with profiler.profile("O1-fare-anomaly-optimized"):
        o1_base_opt = (m1_clean_trips
                       .filter(sf.col("trip_distance") > 0)
                       .withColumn("fare_per_dist", sf.col("fare_amount") / sf.col("trip_distance"))
                       .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
                       .withColumn("is_weekend", sf.dayofweek(sf.col("tpep_pickup_datetime")).isin([1, 7]))
                       )

        o1_stats_opt = (o1_base_opt
        .groupBy("PULocationID", "hour_of_day")
        .agg(
            sf.mean("fare_per_dist").alias("mean_fare_per_dist"),
            sf.stddev("fare_per_dist").alias("std_fare_per_dist")
        )
        )

        o1_flagged_opt = (o1_base_opt
        .join(sf.broadcast(o1_stats_opt), on=["PULocationID", "hour_of_day"], how="inner")
        .join(sf.broadcast(zone_lookup), on="PULocationID", how="inner")
        .withColumn(
            "is_fare_anomaly",
            sf.when(
                sf.col("std_fare_per_dist").isNotNull() & (sf.col("std_fare_per_dist") > 0),
                sf.col("fare_per_dist") > (sf.col("mean_fare_per_dist") + (3 * sf.col("std_fare_per_dist")))
            ).otherwise(False)
        )
        )

        o1_summary_opt = (o1_flagged_opt
                          .groupBy("pickup_borough", "payment_type", "is_weekend")
                          .agg(
            sf.count("*").alias("total_trips"),
            sf.sum(sf.when(sf.col("is_fare_anomaly"), 1).otherwise(0)).alias("anomaly_trips"),
            (sf.sum(sf.when(sf.col("is_fare_anomaly"), 1).otherwise(0)) / sf.count("*")).alias("anomaly_rate")
        )
                          .orderBy("pickup_borough", "payment_type", "is_weekend")
                          )
        o1_summary_opt.show(10, truncate=False)

    print(profiler.compare("O1-fare-anomaly-baseline", "O1-fare-anomaly-optimized"))

    # ==========================================
    # O2: Flow Analysis (Baseline & Optimized)
    # ==========================================
    print("\nStarting O2 Baseline...")
    spark.sparkContext.setJobGroup("O2_FLOWS_BASELINE", "O2: Baseline")

    with profiler.profile("O2-origin-destination-baseline"):
        o2_pairs = (m1_clean_trips
                    .withColumn("year_month", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyy-MM"))
                    .groupBy("PULocationID", "DOLocationID", "year_month")
                    .agg(sf.count("*").alias("pair_trip_count"))
                    )

        zone_pickup = zone_lookup_full.select(sf.col("zone_id").alias("PULocationID"),
                                              sf.col("borough").alias("pickup_borough"),
                                              sf.col("zone_name").alias("pickup_zone"))
        zone_dropoff = zone_lookup_full.select(sf.col("zone_id").alias("DOLocationID"),
                                               sf.col("borough").alias("dropoff_borough"),
                                               sf.col("zone_name").alias("dropoff_zone"))

        o2_named = o2_pairs.join(zone_pickup, on="PULocationID", how="inner").join(zone_dropoff, on="DOLocationID",
                                                                                   how="inner")

        w_pickup_month = Window.partitionBy("PULocationID", "year_month")
        o2_shares = (o2_named
                     .withColumn("total_outbound_trips", sf.sum("pair_trip_count").over(w_pickup_month))
                     .withColumn("route_outbound_share", sf.col("pair_trip_count") / sf.col("total_outbound_trips"))
                     )

        w_borough_rank = Window.partitionBy("pickup_borough", "year_month").orderBy(sf.col("pair_trip_count").desc())
        o2_top20 = o2_shares.withColumn("borough_route_rank", sf.rank().over(w_borough_rank)).filter(
            sf.col("borough_route_rank") <= 20)

        o2_quarters = (o2_top20
                       .withColumn("quarter", sf.quarter(sf.to_date(sf.col("year_month"), "yyyy-MM")))
                       .filter(sf.col("quarter").isin([1, 3]))
                       .groupBy("PULocationID", "DOLocationID", "pickup_borough", "pickup_zone", "dropoff_zone",
                                "quarter")
                       .agg(sf.avg("route_outbound_share").alias("avg_quarter_share"))
                       )

        o2_q1 = o2_quarters.filter(sf.col("quarter") == 1).alias("q1")
        o2_q3 = o2_quarters.filter(sf.col("quarter") == 3).alias("q3")

        o2_reversals = (o2_q1
                        .join(o2_q3, (sf.col("q1.PULocationID") == sf.col("q3.PULocationID")) & (
                    sf.col("q1.DOLocationID") == sf.col("q3.DOLocationID")), how="inner")
                        .withColumn("share_delta", sf.col("q3.avg_quarter_share") - sf.col("q1.avg_quarter_share"))
                        .select(sf.col("q1.pickup_borough"), sf.col("q1.pickup_zone"), sf.col("q1.dropoff_zone"),
                                sf.col("q1.avg_quarter_share").alias("q1_share"),
                                sf.col("q3.avg_quarter_share").alias("q3_share"), "share_delta")
                        .orderBy(sf.abs(sf.col("share_delta")).desc())
                        )
        o2_reversals.show(10, truncate=False)

    print("\nStarting O2 Optimized...")
    spark.sparkContext.setJobGroup("O2_FLOWS_OPTIMIZED", "O2: Optimized")

    with profiler.profile("O2-origin-destination-optimized"):
        o2_pairs_opt = (m1_clean_trips
                        .withColumn("year_month", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyy-MM"))
                        .groupBy("PULocationID", "DOLocationID", "year_month")
                        .agg(sf.count("*").alias("pair_trip_count"))
                        )

        zone_borough_bc = sf.broadcast(
            zone_lookup_full.select(sf.col("zone_id").alias("PULocationID"), sf.col("borough").alias("pickup_borough")))
        o2_with_borough = o2_pairs_opt.join(zone_borough_bc, on="PULocationID", how="inner")

        w_pickup_month = Window.partitionBy("PULocationID", "year_month")
        w_borough_rank = Window.partitionBy("pickup_borough", "year_month").orderBy(sf.col("pair_trip_count").desc())

        o2_top20_opt = (o2_with_borough
                        .withColumn("total_outbound_trips", sf.sum("pair_trip_count").over(w_pickup_month))
                        .withColumn("route_outbound_share", sf.col("pair_trip_count") / sf.col("total_outbound_trips"))
                        .withColumn("borough_route_rank", sf.rank().over(w_borough_rank))
                        .filter(sf.col("borough_route_rank") <= 20)
                        )

        o2_reversals_raw = (o2_top20_opt
                            .withColumn("quarter", sf.quarter(sf.to_date(sf.col("year_month"), "yyyy-MM")))
                            .filter(sf.col("quarter").isin([1, 3]))
                            .groupBy("PULocationID", "DOLocationID", "pickup_borough")
                            .agg(
            sf.avg(sf.when(sf.col("quarter") == 1, sf.col("route_outbound_share"))).alias("q1_share"),
            sf.avg(sf.when(sf.col("quarter") == 3, sf.col("route_outbound_share"))).alias("q3_share")
        )
                            .filter(sf.col("q1_share").isNotNull() & sf.col("q3_share").isNotNull())
                            .withColumn("share_delta", sf.col("q3_share") - sf.col("q1_share"))
                            )

        zone_p_bc = sf.broadcast(
            zone_lookup_full.select(sf.col("zone_id").alias("PULocationID"), sf.col("zone_name").alias("pickup_zone")))
        zone_d_bc = sf.broadcast(
            zone_lookup_full.select(sf.col("zone_id").alias("DOLocationID"), sf.col("zone_name").alias("dropoff_zone")))

        o2_reversals_opt = (o2_reversals_raw
                            .join(zone_p_bc, on="PULocationID", how="inner")
                            .join(zone_d_bc, on="DOLocationID", how="inner")
                            .select("pickup_borough", "pickup_zone", "dropoff_zone", "q1_share", "q3_share",
                                    "share_delta")
                            .orderBy(sf.abs(sf.col("share_delta")).desc())
                            )
        o2_reversals_opt.show(10, truncate=False)

    print(profiler.compare("O2-origin-destination-baseline", "O2-origin-destination-optimized"))
    spark.sparkContext.setJobGroup("", "")


if __name__ == "__main__":
    main()