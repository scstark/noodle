# stark_challenge.py


from os.path import expanduser, join, abspath

# initialize Spark Session
from pyspark.sql import SparkSession
# various Spark SQL functions
from pyspark.sql.types import FloatType
from pyspark.sql.functions import to_date

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Coding Challenge") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# read data from WeatherRaw using Hive
#spark.sql("SELECT * FROM weatherraw").show()

weather_df = spark.sql("SELECT * FROM weatherraw")

# transform data

# cast 'observationdate' from String to Date
weather_df = weather_df.withColumn('observationdate',to_date(weather_df.observationdate, 'yyyyMMdd'))

# cast 'observationvalue' to float in preparation for pivot

weather_df = weather_df.withColumn('observationvalue', weather_df["observationvalue"].cast(FloatType()))

weather_df.printSchema()

weather_df_pivot = weather_df.groupby(["stationidentifier","observationdate"]).pivot("observationtype", ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN", "EVAP", "WESD", "WESF", "PSUN"]).sum("observationvalue")

weather_df_pivot.printSchema()

weather_df_pivot.show()

# adjust columns for unit criteria

cols_in_tenths = ["TMAX", "TMIN", "EVAP", "WESD", "WESF"]

for item in cols_in_tenths:
	weather_df_pivot = weather_df_pivot.withColumn(item, weather_df_pivot[item] * float(0.1))

new_col_names = {
	"PRCP": "Precipitation",
	"SNOW": "Snowfall",
	"SNWD": "SnowDepth",
	"TMAX": "MaxTemperature",
	"TMIN": "MinTemperature",
	"EVAP": "Evaporation",
	"WESD": "WaterEquivalentSnowDepth",
	"WESF": "WaterEquivalentSnowFall",
	"PSUN": "Sunshine"
}

# rename columns as specified for output table
for item in new_col_names.keys():
	weather_df_pivot = weather_df_pivot.withColumnRenamed(item,new_col_names[item])

weather_df_pivot.printSchema()

# save to Hive as table 'WeatherCurated'
weather_df_pivot.createOrReplaceTempView("WeatherTempTable")

spark.sql("create table WeatherCurated as select * from WeatherTempTable")