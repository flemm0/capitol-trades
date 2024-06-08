CREATE EXTERNAL TABLE IF NOT EXISTS `capitol-trades`.`trades_cleaned` (
  `politician` varchar(55),
  `party` varchar(25),
  `chamber` varchar(25),
  `issuer_name` varchar(200),
  `issuer_ticker` varchar(25),
  `published_date` date,
  `traded_date` date,
  `filed_after` int,
  `owner` varchar(25),
  `type` varchar(25),
  `size` varchar(25),
  `price` float,
  `politician_first_name` varchar(35),
  `politician_last_name` varchar(35)
) COMMENT "transformed data scraped from capitol trades website"
PARTITIONED BY (state string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://capitol-trades-data-lake/trades_parquet/'
TBLPROPERTIES ('classification' = 'parquet');

MSCK REPAIR TABLE `capitol-trades`.`trades_cleaned`;
