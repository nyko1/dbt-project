-- Phase de Test
-- SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, total_amount
-- FROM {{ source('tlc_taxi_trips', 'raw_yellow_tripdata')}}
-- LIMIT 10

-- Configuration du modèle dbt pour la matérialisation en tant que fichier externe au format Parquet, 
--avec une localisation spécifiée pour le fichier de sortie. 
--Cette configuration permet de stocker les résultats de la transformation dans un fichier Parquet, 
--ce qui est efficace pour le stockage et l'analyse de grandes quantités de données.
{{ config(
    materialized = 'external',
    location = "output/taxi_2024_transformed.parquet",
    file_format = 'parquet'
) }}

-- Phase de transformation

-- Définition des CTE
-- 1. source_data : sélectionne les données brutes à partir de la source définie dans sources.yml, en excluant les colonnes VendorID et RateCodeID.
WITH source_data AS (
    SELECT * EXCLUDE (VendorID, RateCodeID)
    FROM {{ source('tlc_taxi_trips', 'raw_yellow_tripdata')}}
),

-- Filtrage des données pour exclure les valeurs aberrantes
-- 2. filtered_data : applique des conditions pour filtrer les données, notamment en excluant les trajets avec des distances négatives ou nulles, des montants totaux négatifs ou nuls, et des enregistrements où l'heure de prise en charge est après l'heure de dépôt. De plus, on s'assure que le champ store_and_fwd_flag est égal à 'N', que le tip_amount est supérieur ou égal à 0, et que le payment_type est soit 1 (Credit Card) soit 2 (Cash).
filtered_data AS (
    SELECT *
    FROM source_data
    WHERE 
      passenger_count > 0
      AND trip_distance > 0
      AND total_amount > 0
      AND tpep_pickup_datetime < tpep_dropoff_datetime
      AND store_and_fwd_flag = 'N'
      AND tip_amount >= 0
      AND payment_type IN (1, 2)
),

-- Résultat final après transformation
-- 3. transformed_data : effectue des transformations supplémentaires sur les données filtrées, notamment en convertissant le champ passenger_count en BIGINT, en créant une nouvelle colonne payment_method qui traduit les codes de paiement en texte (1 devient 'Credit Card' et 2 devient 'Cash'), et en calculant la durée du trajet en minutes à partir des champs tpep_pickup_datetime et tpep_dropoff_datetime. Les autres colonnes sont conservées telles quelles.
transformed_data AS (
    SELECT 
      CAST(passenger_count AS BIGINT) AS passenger_count,

      CASE WHEN payment_type = 1 THEN 'Credit Card' 
           WHEN payment_type = 2 THEN 'Cash' 
           END AS payment_method,

      DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,

      * EXCLUDE (passenger_count, payment_type)

    FROM filtered_data
),

final_data AS (
    SELECT *, 
        CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
        CAST(tpep_dropoff_datetime AS DATE) AS dropoff_date
    FROM transformed_data
    WHERE pickup_date >= '2024-01-01' AND pickup_date <= '2024-12-31'
        AND dropoff_date >= '2024-01-01' AND dropoff_date <= '2024-12-31'
)

-- Sélection des données transformées
SELECT * EXCLUDE (pickup_date, dropoff_date)
FROM final_data
WHERE trip_duration_minutes > 0