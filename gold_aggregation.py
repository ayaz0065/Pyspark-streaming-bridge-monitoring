import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, col
from pyspark.sql.functions import max as smax
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


def get_spark(app_name: str):
    spark = (SparkSession.builder.appName(app_name)
             .config('spark.sql.shuffle.partitions','4')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark


silver_parquet_schema = StructType([
    StructField('event_time',     StringType(),   True),
    StructField('bridge_id',      IntegerType(),  True),
    StructField('sensor_type',    StringType(),   True),
    StructField('value',          DoubleType(),   True),
    StructField('ingest_time',    StringType(),   True),
    StructField('event_time_ts',  TimestampType(),True),
    StructField('ingest_time_ts', TimestampType(),True),
    StructField('name',           StringType(),   True),
    StructField('location',       StringType(),   True),
    StructField('installation_date', StringType(),True),
])


def agg_stream(spark, base, silver_dir, metric_colname, agg_expr):
    df = (spark.readStream.format('parquet')
          .schema(silver_parquet_schema)
          .load(f"{base}/{silver_dir}")
          .withWatermark('event_time_ts', '2 minutes'))

    agg = (df.groupBy(window(col('event_time_ts'), '1 minute'), col('bridge_id'))
           .agg(agg_expr.alias(metric_colname))
           .select(col('bridge_id'),
                   col('window.start').alias('window_start'),
                   col('window.end').alias('window_end'),
                   col(metric_colname)))
    return agg


def main():
    ap = argparse.ArgumentParser(description='Gold aggregations (1m windows + 2m watermark + joins)')
    ap.add_argument('--base', required=True)
    ap.add_argument('--run-seconds', type=int, default=0)
    args = ap.parse_args()

    spark = get_spark('GoldAggregation')

    temp_agg = agg_stream(spark, args.base, 'silver/temperature', 'avg_temperature', avg('value'))
    vib_agg  = agg_stream(spark, args.base, 'silver/vibration',  'max_vibration',   smax('value'))
    tilt_agg = agg_stream(spark, args.base, 'silver/tilt',       'max_tilt',        smax('value'))

    tv  = temp_agg.join(vib_agg, on=['bridge_id','window_start','window_end'], how='inner')
    tvt = tv.join(tilt_agg,       on=['bridge_id','window_start','window_end'], how='inner')

    q = (tvt.writeStream.format('parquet')
         .option('checkpointLocation', f"{args.base}/checkpoints/gold_metrics")
         .outputMode('append')
         .start(f"{args.base}/gold/bridge_metrics"))

    if args.run_seconds:
        spark.streams.awaitAnyTermination(args.run_seconds*1000)
        for q in spark.streams.active:
            try: q.stop()
            except: pass
    else:
        spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
