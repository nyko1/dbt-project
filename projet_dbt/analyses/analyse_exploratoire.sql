-- Acceder au repertoire d'analyse exploratoire à partir de duckdbt cli
-- .cd D:/NYKO/Projets/dbt-project/projet_dbt/analyses


-- SELECT * 
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
-- LIMIT 10;

-- SELECT COUNT(*) FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet' LIMIT 10;

-- SELECT VendorID, COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- GROUP BY VendorID;

-- SELECT RateCodeID, COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- GROUP BY RateCodeID;

-- SELECT store_and_fwd_flag, COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- GROUP BY store_and_fwd_flag;

-- SELECT payment_type, COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- GROUP BY payment_type;

-- SELECT PULocationID, COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- GROUP BY PULocationID;

-- SELECT DOLocationID, COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- GROUP BY DOLocationID;

-- S'assurer que l'heure de prise en charge est avant l'heure de dépôt
-- SELECT COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- WHERE tpep_pickup_datetime > tpep_dropoff_datetime;

-- SELECT *
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- WHERE tpep_pickup_datetime > tpep_dropoff_datetime
-- LIMIT 10;

-- SELECT COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- WHERE trip_distance <= 0;

-- SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- WHERE trip_distance < 0
-- LIMIT 10;

-- SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- WHERE trip_distance = 0
-- LIMIT 10;



-- SELECT COUNT(*) AS trip_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- WHERE total_amount <= 0;

-- SELECT trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime, total_amount, passenger_count
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
-- WHERE total_amount < 0
-- LIMIT 10;



--Selectionner toutes les colonnes et en exclures quelques unes pour voir les données
SELECT * EXCLUDE (VendorID, RateCodeID, store_and_fwd_flag, payment_type, PULocationID, DOLocationID)
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
LIMIT 10; 

-- Conclusion :
-- 1. Les données contiennent des valeurs aberrantes, notamment des trajets avec des distances négatives ou nulles, des montants totaux négatifs ou nuls, et des enregistrements où l'heure de prise en charge est après l'heure de dépôt.
