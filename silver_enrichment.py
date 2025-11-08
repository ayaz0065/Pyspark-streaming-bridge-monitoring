#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


def get_spark(app_name: str):
    spark = (SparkSession.builder.appName(app_name)
             .config('spark.sql.shuffle.partitions','4')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark


def write_batch_metrics(df, epoch_id, base, sensor, layer, status):
    from pyspark.sql import Row
    ts = int(__import__('time').time()*1000)
    cnt = df.count()
    spark = df.sparkSession
    spark.createDataFrame([Row(ts=ts, layer=layer, sensor=sensor, status=status, rows=cnt)]) \
        .write.mode('append').parquet(f"{base}/monitoring/metrics")


bronze_parquet_schema = StructType([
    StructField('event_time',     StringType(),   True),
    StructField('bridge_id',      IntegerType(),  True),
    StructField('sensor_type',    StringType(),   True),
    StructField('value',          DoubleType(),   True),
    StructField('ingest_time',    StringType(),   True),
    StructField('event_time_ts',  TimestampType(),True),
    StructField('ingest_time_ts', TimestampType(),True),
])


def silver_stream(spark, base, bronze_dir, out_dir, ckpt_name, sensor):
    sdf = (spark.readStream.format('parquet')
           .schema(bronze_parquet_schema)
           .load(f"{base}/{bronze_dir}")
           .filter(col('sensor_type') == sensor))

    metadata_df = (spark.read.format('csv').option('header', True)
                   .load(f"{base}/metadata/bridges.csv")
                   .select(col('bridge_id').cast('int').alias('b_bridge_id'), 'name','location','installation_date'))

    enriched = (sdf.join(metadata_df, sdf.bridge_id == col('b_bridge_id'), 'left')
                .drop('b_bridge_id'))

    if sensor == 'temperature':
        rule = col('value').between(-40, 80)
    elif sensor == 'vibration':
        rule = (col('value') >= 0)
    else:
        rule = col('value').between(0, 90)

    cond = col('event_time_ts').isNotNull() & rule & col('name').isNotNull()

    valid = enriched.filter(cond)
    rejected = (enriched.filter(~cond)
                .withColumn('reject_reason',
                            when(col('name').isNull(),'no metadata match')
                            .when(~rule, 'range violation')
                            .when(col('event_time_ts').isNull(), 'bad timestamp')
                            .otherwise('dq fail')))

    ok_q = (valid.writeStream.format('parquet')
            .option('checkpointLocation', f"{base}/checkpoints/{ckpt_name}_ok")
            .outputMode('append').start(f"{base}/{out_dir}"))

    rj_q = (rejected.writeStream.format('parquet')
            .option('checkpointLocation', f"{base}/checkpoints/{ckpt_name}_rej")
            .outputMode('append').start(f"{base}/silver/rejected"))

    # metrics
    m_ok = valid.writeStream.foreachBatch(lambda d,e: write_batch_metrics(d,e,base,sensor,'silver','valid')).start()
    m_rj = rejected.writeStream.foreachBatch(lambda d,e: write_batch_metrics(d,e,base,sensor,'silver','rejected')).start()

    return [ok_q, rj_q, m_ok, m_rj]


def main():
    ap = argparse.ArgumentParser(description='Silver enrichment (stream-static join + rules)')
    ap.add_argument('--base', required=True)
    ap.add_argument('--run-seconds', type=int, default=0)
    args = ap.parse_args()

    spark = get_spark('SilverEnrichment')

    qs = []
    qs += silver_stream(spark, args.base, 'bronze/temperature', 'silver/temperature', 'silver_temp', 'temperature')
    qs += silver_stream(spark, args.base, 'bronze/vibration',  'silver/vibration',  'silver_vib',  'vibration')
    qs += silver_stream(spark, args.base, 'bronze/tilt',       'silver/tilt',       'silver_tilt', 'tilt')

    if args.run_seconds:
        spark.streams.awaitAnyTermination(args.run_seconds*1000)
        for q in spark.streams.active:
            try: q.stop()
            except: pass
    else:
        spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
