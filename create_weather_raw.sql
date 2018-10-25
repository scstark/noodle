CREATE EXTERNAL TABLE IF NOT EXISTS WeatherRaw (
  StationIdentifier STRING,
  ObservationDate STRING,
  ObservationType STRING,
  ObservationValue STRING,
  ObservationTime STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/noodle/raw/'
;