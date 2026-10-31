# Lakebridge ETL

Pipelines de synchronisation entre Delta Lake, PostgreSQL et une API de données métier.

Version de portfolio maintenue sur [rodriguemakeit-oss](https://github.com/rodriguemakeit-oss).

## Ce que le projet explore

Pipelines de synchronisation entre Delta Lake, PostgreSQL et une API de données métier. L’accent est mis sur la séparation des composants, la configuration explicite et la réutilisation des traitements.

## Structure

`src/delta_to_rds/ · src/kpi_data_to_rds/ · src/pg_kpi_data_to_delta_table/`

## Prise en main

Consultez [GUIDE.md](GUIDE.md) pour les commandes et la configuration détaillées.

Les valeurs de déploiement sont des exemples. Les identifiants nuls et les domaines `example.invalid` doivent être remplacés dans votre propre environnement. Aucun déploiement cloud n’est lancé automatiquement.

## État du projet

Cette adaptation nécessite vos propres services et jeux de données. Les contrôles locaux sont décrits dans [VALIDATION.md](VALIDATION.md) ; ils ne constituent pas une validation des intégrations cloud.

## Contributions et licences

Les noms de packages, crédits d’auteurs et avis tiers conservés dans les sources s’appliquent à leurs composants respectifs. Aucune licence globale supplémentaire n’est attribuée à ce dépôt.
