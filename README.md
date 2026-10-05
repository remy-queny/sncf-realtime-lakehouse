# SNCF Real-Time Lakehouse

Projet de data engineering local pour analyser des événements de retard ferroviaire avec **Java, Kafka, PySpark et Delta Lake**.

Le pipeline suit une architecture Bronze / Silver / Gold et publie les indicateurs dans PostgreSQL pour leur consultation avec Metabase.

> État actuel : pipeline implémenté en mode simulation, publication PostgreSQL disponible, Metabase intégré à Docker Compose et tests Java / Python présents. La connexion aux données SNCF réelles et la CI restent à réaliser. Le dépôt fourni ne contient pas d'export permettant de vérifier la configuration du dashboard Metabase.

## 1. Objectif et architecture

### Question métier

> Quelles lignes et quels arrêts concentrent les retards sur une période donnée ?

Indicateurs implémentés :

- Nombre d'événements par ligne et par fenêtre de 15 minutes.
- Nombre de courses distinctes avec plus de cinq minutes de retard par ligne et par fenêtre.
- Retard moyen et maximal par ligne.
- Nombre d'événements en retard, retard moyen et maximal par arrêt et par jour.

Les événements et les référentiels utilisés actuellement sont simulés. Ils ne constituent pas des mesures réelles de la ponctualité SNCF.

### Flux de données

```text
Producteur Java / simulateur Python
                  |
                  v
       Kafka : sncf.trip_updates.raw
                  |
                  v
       Bronze Delta : payload brut
                  |
                  v
       Silver Delta : validation, déduplication,
                      enrichissement par CSV simulés
                  |
                  v
       Gold Delta : agrégats par ligne et par arrêt
                  |
                  v
       PostgreSQL : publication par upsert
                  |
                  v
       Metabase : consultation après configuration
```

Bronze et Silver utilisent Structured Streaming avec `availableNow=True` : chaque lancement traite les données disponibles puis se termine. Les agrégats Gold sont recalculés en batch, puis publiés dans PostgreSQL. La version actuelle n'est donc pas un dashboard alimenté en continu par des jobs permanents.

### Organisation du dépôt

```text
.
├── docker-compose.yml             # Kafka, Kafka UI, PostgreSQL et Metabase
├── .env.example                   # Exemple de configuration locale
├── requirements-simulator.txt     # Dépendances du simulateur Python
├── docs/
│   └── data-contract.md           # Contrat JSON des événements
├── infra/postgres/
│   └── 001_gold_tables.sql        # Tables de restitution
├── producer-java/                 # Producteur Java et tests JUnit
├── scripts/
│   ├── create_topics.sh           # Création du topic Kafka
│   ├── run_demo.sh                # Un événement Java + pipeline complet
│   ├── run_pipeline.sh            # Traitements et publication PostgreSQL
│   ├── simulate_trip_updates.py   # Événements valides ou invalides
│   └── snapshot_resume_state.py   # Capture de l'état Silver / PostgreSQL
├── spark/
│   ├── requirements.txt
│   ├── resources/simulation/      # Référentiels CSV des lignes et arrêts
│   ├── src/jobs/                  # Ingestion, agrégations et publication
│   ├── src/transforms/            # Transformations et règles métier
│   └── tests/                     # Tests Python
└── data/
    ├── lakehouse/                 # Tables Delta générées localement
    ├── checkpoints/               # État des requêtes streaming
    └── validation/                # Captures de contrôle de reprise
```

## 2. Stack et modèle de données

### Technologies

| Composant | Technologie | Rôle |
|---|---|---|
| Infrastructure | Docker Compose | Services locaux et volumes persistants |
| Bus d'événements | Apache Kafka 4.1.0, KRaft | Transport des messages |
| Inspection Kafka | Kafka UI | Consultation des topics et messages |
| Producteur principal | Java 17, Maven, Jackson | Génération et publication d'événements JSON |
| Simulateur complémentaire | Python, confluent-kafka | Génération de volumes et de cas invalides |
| Traitements | PySpark 3.5.3 | Ingestion, transformations et agrégations |
| Stockage analytique | Delta Lake 3.2.0 | Couches Bronze / Silver / Gold |
| Restitution | PostgreSQL 17, psycopg | Publication des indicateurs |
| Visualisation | Metabase | Exploration des tables PostgreSQL |
| Tests | JUnit 5, pytest | Vérification du producteur et des transformations |

Kafka, Kafka UI, PostgreSQL et Metabase tournent dans Docker. Le producteur Java et les jobs Spark s'exécutent sur la machine hôte ; Spark utilise le mode `local[*]`.

### Couches du lakehouse

| Couche | Table / dossier | Contenu |
|---|---|---|
| Bronze | `bronze/trip_updates` | Payload JSON original, clé Kafka, topic, partition, offset et timestamps |
| Silver | `silver/trip_delays` | Événements typés, retards en minutes et noms de lignes / arrêts |
| Silver | `silver/rejected_events` | Payloads rejetés, métadonnées Kafka et motif de validation |
| Gold | `gold/delay_by_line_15min` | Agrégats par ligne et fenêtre de 15 minutes |
| Gold | `gold/station_delay_daily` | Agrégats quotidiens par arrêt |

Tables PostgreSQL : `gold_delay_by_line_15min` et `gold_station_delay_daily`.

### Contrat et règles métier

Le format JSON est décrit dans [le contrat de données](docs/data-contract.md).

- Topic actuel : `sncf.trip_updates.raw`.
- Clé Kafka : `trip_id`.
- Identifiant de déduplication : `event_id`.
- Conversion : `delay_minutes = delay_seconds / 60.0`.
- Retard significatif : `delay_seconds > 300`, soit strictement plus de cinq minutes.
- Silver applique un watermark d'un jour et `dropDuplicatesWithinWatermark` sur `event_id`.
- Les noms de lignes et d'arrêts proviennent des CSV de `spark/resources/simulation/`, pas d'un GTFS réel.

Les compteurs d'événements ne doivent pas être interprétés comme des nombres de trains uniques. `delayed_trip_count` compte les courses distinctes en retard dans chaque fenêtre ; `delayed_event_count` compte les événements en retard par arrêt et par jour.

## 3. Installation et démonstration

### Prérequis

- Docker et Docker Compose.
- JDK 17 et Maven.
- Python 3.11 pour l'environnement local.
- Bash, notamment sur macOS ou Linux.
- Accès Internet à la première installation pour télécharger les images et dépendances.

Exécuter les commandes suivantes à la racine du dépôt.

### Étape 1 — Configurer l'environnement

```bash
cp .env.example .env
```

Modifier le mot de passe PostgreSQL dans `.env`, puis charger les variables :

```bash
set -a
source .env
set +a

python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r spark/requirements.txt
python -m pip install -r requirements-simulator.txt
```

Le script `run_pipeline.sh` utilise actuellement PostgreSQL sur `localhost:5432`. Conserver `POSTGRES_PORT=5432` pour cette procédure. Le topic créé par `create_topics.sh` est également fixé à `sncf.trip_updates.raw`.

### Étape 2 — Démarrer les services

```bash
docker compose up -d
docker compose ps
```

Attendre que Kafka et PostgreSQL soient prêts avant de poursuivre. Pour consulter leurs logs :

```bash
docker compose logs --tail=100 kafka postgres
```

### Étape 3 — Initialiser Kafka et PostgreSQL

```bash
bash scripts/create_topics.sh

docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < infra/postgres/001_gold_tables.sql
```

Le schéma PostgreSQL est initialisé explicitement : le fichier SQL n'est pas monté comme script d'initialisation dans le Docker Compose actuel.

### Étape 4 — Exécuter la démonstration

```bash
bash scripts/run_demo.sh
```

Ce script vérifie le topic, publie un événement simulé avec Java, puis enchaîne Bronze, Silver, les deux agrégations Gold, la publication PostgreSQL et les contrôles Silver.

Il suppose que les services, le topic, les tables PostgreSQL et l'environnement `.venv` sont déjà prêts.

Pour produire davantage de données puis les traiter :

```bash
python scripts/simulate_trip_updates.py --count 100
bash scripts/run_pipeline.sh
```

Pour retraiter uniquement les nouvelles données disponibles et republier les agrégats :

```bash
bash scripts/run_pipeline.sh
```

### Étape 5 — Consulter les résultats

- [Kafka UI](http://localhost:8080) : inspection du topic et des messages.
- [Metabase](http://localhost:3000) : configuration initiale et consultation des indicateurs.

Dans Metabase, ajouter une connexion PostgreSQL avec les paramètres suivants :

| Paramètre | Valeur |
|---|---|
| Hôte | `postgres` |
| Port | `5432` |
| Base | Valeur de `POSTGRES_DB` dans `.env` |
| Utilisateur | Valeur de `POSTGRES_USER` dans `.env` |
| Mot de passe | Valeur de `POSTGRES_PASSWORD` dans `.env` |

Utiliser `postgres`, et non `localhost`, car Metabase et PostgreSQL communiquent sur le réseau Docker.

Les visualisations se configurent dans l'interface Metabase. Aucun dashboard préconfiguré n'est provisionné par les scripts fournis.

## 4. Tests et fiabilité

### Tests Java et Python

```bash
(cd producer-java && mvn test)

PYTHONPATH="$PWD/spark/src" \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest spark/tests -v
```

Les suites présentes couvrent notamment la sérialisation JSON, la cohérence des événements Java, la couverture des référentiels de simulation, les transformations et agrégations Python ainsi que des contrôles qualité Silver.

Ces commandes permettent de vérifier les tests localement ; la présence des tests dans le dépôt ne constitue pas une preuve de leur réussite dans tous les environnements. Aucune CI GitHub Actions n'est fournie dans l'état actuel.

### Contrôle des événements invalides

```bash
python scripts/simulate_trip_updates.py --count 10 --invalid
bash scripts/run_pipeline.sh
```

Le simulateur injecte des cas avec `trip_id` absent, retard non numérique ou timestamp invalide. Silver conserve les rejets dans `silver/rejected_events` avec leur payload et un motif d'erreur.

Le contrôle final `inspect_silver.py` vérifie les doublons d'`event_id`, certains champs obligatoires et la conversion secondes / minutes.

### Reprise après relancement

Les requêtes Bronze et Silver disposent de checkpoints distincts. PostgreSQL utilise des upserts sur les clés `(window_start, route_id)` et `(event_date, stop_id)`.

Les captures `data/validation/reprise_before.json` et `reprise_after.json` présentent le même état Silver / PostgreSQL pour le scénario fourni. Elles documentent un contrôle local, pas une garantie universelle d'exactly-once.

Pour reproduire un contrôle sans produire de nouvel événement, choisir deux noms de fichiers encore inexistants :

```bash
export PYTHONPATH="$PWD/spark/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"

.venv/bin/python scripts/snapshot_resume_state.py data/validation/check_before.json
bash scripts/run_pipeline.sh
.venv/bin/python scripts/snapshot_resume_state.py data/validation/check_after.json

diff -u data/validation/check_before.json data/validation/check_after.json
```

Ne pas supprimer les checkpoints pour un simple redémarrage : ils conservent l'état de progression des traitements. La déduplication Silver est bornée par le watermark, et non garantie sans limite temporelle.

Pour arrêter les services sans supprimer leurs volumes :

```bash
docker compose down
```

## 5. Avancement et prochaines étapes

### Implémenté dans le dépôt

- [x] Infrastructure Docker Compose avec Kafka, Kafka UI, PostgreSQL et Metabase.
- [x] Topic Kafka créé par un script versionné.
- [x] Contrat d'événement JSON documenté.
- [x] Producteur Java en mode simulation.
- [x] Simulateur Python avec événements valides et invalides.
- [x] Ingestion Kafka vers Bronze Delta.
- [x] Validation, déduplication et enrichissement Silver.
- [x] Conservation des événements rejetés.
- [x] Agrégats Gold par ligne et par arrêt.
- [x] Publication Gold vers PostgreSQL par upsert.
- [x] Scripts de démonstration et de traitement.
- [x] Tests Java et Python.
- [x] Captures de contrôle de reprise Silver / PostgreSQL.

### À finaliser pour la V1

- [ ] Documenter et versionner les preuves du dashboard Metabase : captures, filtres et procédure de création.
- [ ] Ajouter une CI GitHub Actions pour exécuter les tests et vérifier le build.
- [ ] Documenter les résultats des tests et compléter les scénarios de validation.
- [ ] Compléter le runbook et la documentation d'architecture.
- [ ] Connecter une source GTFS-RT réelle tout en conservant le mode simulation.
- [ ] Remplacer ou compléter les CSV simulés par un référentiel GTFS réel.
- [ ] Étendre les contrôles Silver pour couvrir l'ensemble du contrat documenté.

### Limites actuelles

- Le producteur Java publie un événement par exécution ; `SIMULATION_MODE=false` n'est pas implémenté.
- Les jobs sont lancés à la demande : ingestion incrémentale Bronze / Silver, puis Gold et publication en batch.
- Gold est recalculé avec `overwrite` à partir de Silver à chaque lancement.
- La publication PostgreSQL collecte les agrégats sur le driver et refuse plus de 10 000 lignes par table : elle est destinée à la démonstration locale.
- La validation Silver actuelle ne couvre pas encore toutes les règles du contrat de données.
- Les alertes de service, une DLQ Kafka et une orchestration Airflow ne sont pas implémentées.
- L'infrastructure utilise un seul broker Kafka et n'est pas un déploiement de production haute disponibilité.

### Évolutions envisagées

Une fois la V1 finalisée : orchestration des tâches batch et du rafraîchissement des référentiels avec Airflow, observabilité renforcée et documentation d'une éventuelle migration Azure. Le cloud ne constitue pas une dépendance de la version locale.

Ne pas versionner les secrets du fichier `.env`, les données lakehouse volumineuses ni les checkpoints d'exécution.
