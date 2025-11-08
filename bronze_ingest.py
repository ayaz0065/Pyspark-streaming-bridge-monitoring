import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, lit, when


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


def bronze_stream(spark, base, src_subdir, out_subdir, ckpt_name, sensor):
    schema = (StructType()
              .add('event_time', StringType())
              .add('bridge_id',  IntegerType())
              .add('sensor_type',StringType())
              .add('value',      DoubleType())
              .add('ingest_time',StringType()))

    df = (spark.readStream.format('json').schema(schema)
          .option('maxFilesPerTrigger', 50)
          .load(f"{base}/streams/{src_subdir}"))

    bronze = (df
              .withColumn('event_time_ts', to_timestamp('event_time'))
              .withColumn('ingest_time_ts', to_timestamp('ingest_time')))

    cond = (col('event_time_ts').isNotNull() & col('value').isNotNull() & col('bridge_id').isNotNull())
    valid = bronze.filter(cond)
    rejected = (bronze.filter(~cond)
                .withColumn('reject_reason',
                            when(col('event_time_ts').isNull(), 'bad event_time')
                            .when(col('value').isNull(), 'null value')
                            .when(col('bridge_id').isNull(), 'null bridge_id')
                            .otherwise('missing/invalid fields')))

    ok_q = (valid.writeStream.format('parquet')
            .option('checkpointLocation', f"{base}/checkpoints/{ckpt_name}_ok")
            .outputMode('append').start(f"{base}/bronze/{out_subdir}"))

    rj_q = (rejected.writeStream.format('parquet')
            .option('checkpointLocation', f"{base}/checkpoints/{ckpt_name}_rej")
            .outputMode('append').start(f"{base}/bronze/rejected"))

    m_ok = valid.writeStream.foreachBatch(lambda d,e: write_batch_metrics(d,e,base,sensor,'bronze','valid')).start()
    m_rj = rejected.writeStream.foreachBatch(lambda d,e: write_batch_metrics(d,e,base,sensor,'bronze','rejected')).start()

    return [ok_q, rj_q, m_ok, m_rj]


def main():
    ap = argparse.ArgumentParser(description='Bronze ingestion (file streaming)')
    ap.add_argument('--base', required=True)
    ap.add_argument('--run-seconds', type=int, default=0, help='Await termination seconds (0 = forever)')
    args = ap.parse_args()

    spark = get_spark('BronzeIngest')

    qs = []
    qs += bronze_stream(spark, args.base, 'bridge_temperature', 'temperature', 'bronze_temp', 'temperature')
    qs += bronze_stream(spark, args.base, 'bridge_vibration',  'vibration',  'bronze_vib',  'vibration')
    qs += bronze_stream(spark, args.base, 'bridge_tilt',       'tilt',       'bronze_tilt', 'tilt')

    if args.run_seconds:
        spark.streams.awaitAnyTermination(args.run_seconds*1000)
        for q in spark.streams.active:
            try: q.stop()
            except: pass
    else:
        spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
