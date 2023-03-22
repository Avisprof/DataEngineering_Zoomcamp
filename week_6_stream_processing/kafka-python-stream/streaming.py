from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from settings import GREEN_SCHEMA, CONSUME_TOPIC_GREEN
from settings import FHV_SCHEMA, CONSUME_TOPIC_FHV
from settings import RIDE_ALL_SCHEMA, PRODUCE_TOPIC_RIDE_ALL

def read_from_kafka(consume_topic: str, checkpoint='checkpoint'):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", checkpoint) \
        .load()
    return df_stream


def parse_data_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    cols = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, cols.getItem(idx).cast(field.dataType))  

    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query  # pyspark.sql.streaming.StreamingQuery


def sink_memory(df, query_name, query_template):
    query_df = df \
        .writeStream \
        .queryName(query_name) \
        .format("memory") \
        .start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return query_results, query_df


def sink_kafka(df, topic, output_mode='append', checkpoint='checkpoint'):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .outputMode(output_mode) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint) \
        .start()
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    df = df.withColumn("value", F.concat_ws(', ', *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df.key.cast('string'))
    return df.select(['key', 'value'])

def process_stream_data(consume_topic, consume_schema, key_column, source_name, checkpoint):

    # read_streaming data
    df_consume_stream = read_from_kafka(consume_topic=consume_topic)

    # parse streaming data
    df_data = parse_data_from_kafka_message(df_consume_stream, consume_schema)

    #sink_console(df_data, output_mode='append')

    # get aggregated data
    df_agg = df_data \
                .filter(F.col(key_column).isNotNull()) \
                .groupBy(key_column) \
                .count() \
                .withColumnRenamed(key_column,'pickup_id') \
                .withColumn('key', F.col('pickup_id')) \
                .withColumn('source', F.lit(source_name))
    
    # write the output out to the console for debugging / testing
    sink_console(df_agg)

    # prepare data to kafka in the <key, value> forma
    df_trip_count_messages = prepare_df_to_kafka_sink(df=df_agg,
                                                      value_columns=['pickup_id','count','source'], 
                                                      key_column='key') 
    
    # write the output to the kafka topic
    sink_kafka(df=df_trip_count_messages, 
               topic=PRODUCE_TOPIC_RIDE_ALL, 
               output_mode='complete',
               checkpoint=checkpoint)


if __name__ == "__main__":

    spark = SparkSession.builder.appName('green_tripdata').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
     
    # process green_tripdata 
    process_stream_data(consume_topic = CONSUME_TOPIC_GREEN, 
                        consume_schema = GREEN_SCHEMA, 
                        key_column = 'PULocationID', 
                        source_name = 'green_tripdata',
                        checkpoint = 'checkpoint_green')

    # process fhv_tripdata
    process_stream_data(consume_topic = CONSUME_TOPIC_FHV, 
                        consume_schema = FHV_SCHEMA, 
                        key_column = 'PUlocationID', 
                        source_name ='fhv_tripdata',
                        checkpoint = 'checkpoint_fhv')   

    # find most popular pickup location
    df_ride_all_stream = read_from_kafka(consume_topic=PRODUCE_TOPIC_RIDE_ALL)
    df_ride_all = parse_data_from_kafka_message(df_ride_all_stream, RIDE_ALL_SCHEMA)
    #sink_console(df_ride_all, output_mode='append')

    df_agg = df_ride_all \
               .select(['pickup_id','count']) \
               .groupBy('pickup_id') \
               .agg(F.sum('count').alias('count')) \
               .orderBy(F.col('count').desc())
    sink_console(df_agg)
                         
    spark.streams.awaitAnyTermination()
