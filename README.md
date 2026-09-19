# SNCF Real-Time Lakehouse

Plateforme locale de data engineering pour analyser en temps réel les retards ferroviaires.

## Objectif

Répondre à la question métier :

> Quelles lignes et quelles gares concentrent les retards et perturbations en temps réel ?

## Stack V1

- Apache Kafka en KRaft
- Kafka UI
- Java 17 + Maven
- PySpark Structured Streaming
- Delta Lake
- PostgreSQL
- Metabase
- Docker Compose

## Démarrage

```bash
docker compose up -d
```

## État

- [x] Poste de développement configuré
- [ ] Infrastructure Kafka et PostgreSQL
- [ ] Producteur d'événements simulés
- [ ] Producteur Java
- [ ] Pipeline Spark Bronze / Silver / Gold
- [ ] PostgreSQL et Metabase
- [ ] Tests et CI