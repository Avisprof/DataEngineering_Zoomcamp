import pyspark.sql.types as T

INPUT_DATA_PATH_GREEN = '../../resources/green_tripdata_2019-01.csv'
INPUT_DATA_PATH_FHV = '../../resources/fhv_tripdata_2019-01.csv'

BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_PICKUP_ID_COUNT = 'pickup_location_id_counts_windowed'

PRODUCE_TOPIC_GREEN = CONSUME_TOPIC_GREEN = 'green_tripdata'
PRODUCE_TOPIC_FHV = CONSUME_TOPIC_FHV = 'fhv_tripdata'

GREEN_SCHEMA = T.StructType([
     T.StructField('lpep_pickup_datetime', T.TimestampType()),
     T.StructField('lpep_dropoff_datetime', T.TimestampType()),
     T.StructField('PULocationID', T.IntegerType()),
     T.StructField('DOLocationID', T.IntegerType()),
     T.StructField("passenger_count", T.IntegerType()),
     T.StructField("trip_distance", T.FloatType()),
     T.StructField("payment_type", T.IntegerType()),
     T.StructField("total_amount", T.FloatType()),
])

FHV_SCHEMA = T.StructType([
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropOff_datetime', T.TimestampType()),
     T.StructField('PUlocationID', T.IntegerType()),
     T.StructField('DOlocationID', T.IntegerType())
])


