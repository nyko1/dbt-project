# Source des données
`[Data source](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)`

## Placer les fichiers télécharger dans le dossier data

## Exécute les modèles SQL et matérialise les tables/views dans la base (ici DuckDB).
```bash
dbt run
```
##  Attacher la BD qui stock les transformation  de dbt à duckDB
````
ATTACH 'output/transform_data.db' AS transformed_db;
````

## Package pour test unitaires
*1*  Créer un fichier packages.yml
*2* Le renseigner 
*3* Exécuter la commande ``dbt deps`` 

## Supprime les dossiers générés (target/, dbt_packages/).
### Réinitialise l’environnement du projet.
```
dbt clean
```

## Installe ou réinstalle les packages définis dans packages.yml dans dbt_packages/.
```
dbt deps
```

## Parse le projet, valide le YAML, génère le SQL compilé dans target/ sans exécuter les requêtes.
```
dbt compile
```

## Exécute les tests définis dans les fichiers schema.yml et retourne les échecs éventuels.
```
dbt clean
```

## Faire des tests customiser
```
dbt test --select test_passenger_count
```
```
dbt test --select test_trip_distance
```

