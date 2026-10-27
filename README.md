# SNCF Real-Time Lakehouse

Pipeline local de data engineering pour ingérer des observations de retard ferroviaire SNCF, les traiter dans un lakehouse Delta et les explorer avec Tableau Desktop connecté directement à PostgreSQL.

```text
Java → Kafka → PySpark → Delta Lake Bronze / Silver / Gold → PostgreSQL → Tableau Desktop
```

> État au 27 octobre 2026 : parcours réel à la demande validé sur 110 observations issues des instantanés des 8 et 15 octobre ; publication PostgreSQL et restitution Tableau vérifiées. La reprise normale réelle a été validée le 20 octobre par comparaison avant/après. Le contrôle bloquant de complétude de l’enrichissement Silver et ses 14 cas ciblés ont été validés le 27 octobre, puis la suite Python et l’inspection réelle ont été confirmées sans erreur. Le pipeline ne collecte ni ne traite encore automatiquement les nouveaux flux en permanence. Décision de périmètre : ajouter un mode quasi-temps réel local à la V1 ; conserver le mode à la demande pour les tests et rejeux ; réserver Airflow à la V2. Ce nouveau mode reste à concevoir, implémenter et mesurer.

## Aperçu des dashboards Tableau

### Synthèse — retards SNCF sur l’échantillon réel

![Dashboard Tableau de synthèse : observations analysées, retard moyen et maximal, graphiques par ligne et par arrêt](docs/screenshots/tableau-real-overview.png)

- Trois KPI affichés sur les deux lots réunis : 110 observations, retard moyen pondéré de 7,68 minutes et retard maximal de 45 minutes.
- Retard moyen par ligne et par arrêt, avec valeurs affichées.
- Connexion directe à PostgreSQL, sans export CSV intermédiaire.

### Analyse détaillée — retards par arrêt

![Dashboard Tableau d’analyse des arrêts : volume, moyenne, pourcentage au-delà de cinq minutes, maximum et filtre de période](docs/screenshots/tableau-real-station-analysis.png)

- Tableau comparatif par arrêt : volume d’observations, retard moyen pondéré, part des observations > 5 minutes et retard maximal.
- Tri décroissant du retard moyen et filtre « Période d’observation » sur la date d’observation.
- Filtre « Période d’observation » testé sur les dates disponibles : 8 et 15 octobre 2026. Les contrôles SQL de référence donnent respectivement 10 et 100 observations ; la vue doit être comparée au même périmètre.

Les images référencées sont des captures statiques : les filtres ne sont manipulables que dans Tableau. Les captures de travail ont montré les nouvelles données ; les fichiers PNG aux chemins ci-dessus doivent être actualisés avec la version à 110 observations et leur rendu vérifié sur GitHub. Prévoir des captures sans panneaux d’édition, avec la colonne de retard maximal entièrement visible. La présence des liens dans ce README ne prouve pas que les nouveaux exports sont déjà versionnés.

> Périmètre : 110 observations d’arrêt sélectionnées dans deux instantanés, datés des 8 et 15 octobre 2026. Le lot du 15 octobre a été rejoué le 17 octobre : il ne constitue pas une collecte en direct de ce jour-là. Ce résultat démontre le fonctionnement du pipeline et de la BI, pas la ponctualité globale du réseau. Le titre « Real-Time » désigne l’ambition de la plateforme, pas une validation de fonctionnement permanent. Les exécutions démontrées restent à la demande ; le mode quasi-temps réel fait désormais partie du travail restant pour clôturer la V1 sur ce critère.

La restitution principale est désormais Tableau Desktop Free Edition sur Mac, hors Docker. Metabase reste une validation historique et son service est encore présent dans le Docker Compose fourni ; son retrait n’est pas présenté comme réalisé.

## 1. Objectif et architecture

### Question métier

> Quelles lignes et quels arrêts concentrent les retards sur une période donnée ?

Indicateurs implémentés :

- Nombre d'événements par ligne et par fenêtre de 15 minutes.
- Nombre de courses distinctes avec plus de cinq minutes de retard par ligne et par fenêtre.
- Retard moyen et maximal par ligne.
- Nombre d’événements en retard, retard moyen et maximal par arrêt et par jour.
- KPI globaux : volume d’observations, retard moyen pondéré et retard maximal.
- Part des observations d’arrêt avec retard strictement supérieur à cinq minutes.
- Classement détaillé des arrêts et filtre de période sur leur date d’observation.

Deux parcours coexistent :

- Simulation : événements générés et référentiels CSV simulés, pour une démonstration contrôlée.
- Réel : observations issues d’instantanés SNCF GTFS-RT, enrichies avec les références GTFS statiques du lot correspondant.

Le parcours réel validé cumule 10 observations historiques du 8 octobre et 100 observations du 15 octobre. Le second lot couvre plusieurs courses, lignes et retards significatifs. Cet échantillon sélectionné ne constitue pas une mesure représentative de la ponctualité globale du réseau ou des lignes.

### Flux de données

Parcours de simulation :

```text
Producteur Java / simulateur Python
    → Kafka : sncf.trip_updates.raw
    → Bronze Delta : JSON publié et métadonnées Kafka
    → Silver Delta : validation, déduplication, références simulées
    → Gold Delta : agrégats par ligne et par arrêt
    → PostgreSQL : schéma public, publication par upsert
    → Metabase historique : dashboard de simulation
      (la restitution Tableau de simulation n’est pas encore validée)
```

Parcours réel actuellement validé :

```text
Instantanés GTFS-RT et références GTFS statiques du lot
    → Décodage et mapping Java
    → Exports JSONL canoniques
    → Publication Java contrôlée : 10 observations historiques + 100 nouvelles
    → Kafka : sncf.trip_updates.real.raw
    → Bronze Delta dédiée
    → Silver Delta : validation, déduplication, références GTFS réelles
    → Gold Delta dédiée
    → PostgreSQL : schéma sncf_real, publication par upsert
    → Tableau Desktop Free Edition sur Mac : deux dashboards réels distincts
```

Les instantanés Protobuf sont conservés localement séparément de Bronze. Bronze conserve le JSON canonique reçu depuis Kafka, pas directement le fichier GTFS-RT complet.

Bronze et Silver utilisent Structured Streaming avec `availableNow=True` : chaque lancement traite les données disponibles puis se termine. Gold est recalculé en batch, puis publié dans PostgreSQL. Aucun des dashboards n'est actuellement alimenté par des jobs permanents.

Une collecte HTTP séparée est implémentée : `GtfsRealtimeHttpDownloader` télécharge et valide un instantané ; `scripts/collect_real_snapshot.sh` prépare les références et l’export JSONL. Ce script ne publie pas dans Kafka et ne lance ni Spark ni Tableau. Après un envoi contrôlé via `RealJsonlProducer`, `scripts/run_real_pipeline.sh` enchaîne le traitement réel jusqu’à PostgreSQL. L’actualisation de Tableau est déclenchée séparément.

Voir [la validation historique de la source SNCF réelle](docs/real-source.md) pour le périmètre et les résultats du premier test. Certaines pages historiques mentionnent encore Metabase ; la présente version du README décrit l’avancement Tableau.

### Fonctionnement actuel et objectif de clôture V1

| Dimension | État validé | Objectif V1 restant à réaliser |
|---|---|---|
| Collecte | Instantané téléchargé à la demande | Collecter régulièrement des instantanés récents, avec cadence configurable et erreurs gérées |
| Kafka | Publication contrôlée d’un JSONL sélectionné | Publier automatiquement les nouvelles observations, sans renvoyer aveuglément un instantané inchangé |
| Bronze / Silver | Traitement incrémental avec `availableNow=True`, puis arrêt | Maintenir des requêtes actives en micro-batches, avec arrêt et reprise contrôlés |
| Gold / PostgreSQL | Recalcul et publication à la demande | Mettre à jour les indicateurs automatiquement avec une sémantique cumulative correcte |
| Fraîcheur | Non mesurée de bout en bout en fonctionnement actif | Mesurer l’âge de l’observation source et le délai de traversée jusqu’à PostgreSQL |
| Tableau | Deux dashboards et actualisation manuelle validés | Vérifier les indicateurs récents après actualisation ; ne pas annoncer une UI actualisée automatiquement |
| Airflow | Non intégré | Hors V1 ; orchestration des tâches planifiées en V2 |

Le mode actif visé est du quasi-temps réel local : il fonctionne pendant une session de démonstration sur le Mac, sans exigence de disponibilité 24 h/24. Une fréquence de collecte ou une latence cible ne sera annoncée qu’après examen de la source, des ressources et des mesures. La fraîcheur de l’API reste une contrainte externe : un traitement rapide ne rend pas frais un instantané déjà ancien.

Architecture cible du mode actif — non implémentée à ce stade :

```text
Collecteur/producteur Java actif
    → nouveaux instantanés SNCF normalisés
    → Kafka
    → Bronze et Silver maintenus actifs en micro-batches
    → mise à jour automatique et contrôlée de Gold / PostgreSQL
    → Tableau : lecture des données publiées, actualisation visuelle distincte
```

Cette cible s’ajoute au parcours reproductible à la demande ; elle ne remplace pas les commandes de rejeu existantes. Les étapes et critères d’acceptation sont détaillés dans la feuille de route de la section 6. Aucune nouvelle commande de démarrage actif n’est présentée comme disponible avant son implémentation.

### Organisation du dépôt

```text
.
├── .github/workflows/ci.yml          # Tests Java et Python/Spark
├── docker-compose.yml               # Kafka, Kafka UI, PostgreSQL, Metabase
├── .env.example
├── requirements-simulator.txt
├── docs/
│   ├── data-contract.md
│   ├── metabase.md                   # Dashboard de simulation
│   ├── real-source.md                # Validation et limites du parcours réel
│   ├── runbook.md
│   └── screenshots/
│       ├── tableau-real-overview.png
│       └── tableau-real-station-analysis.png
├── infra/postgres/
│   ├── 001_gold_tables.sql           # Tables de simulation dans public
│   └── 002_real_gold_tables.sql      # Tables réelles dans sncf_real
├── bi/tableau/
│   ├── README.md                     # Reconnexion, sources, calculs et filtres
│   └── sncf_real_dashboard.twb        # Classeur local ; vérifier avant versionnement
├── producer-java/
│   ├── pom.xml
│   └── src/                         # Producteurs, lecteurs GTFS, mapper, tests
├── scripts/
│   ├── collect_real_snapshot.sh      # Collecte HTTP + références + JSONL, sans Kafka/Spark
│   ├── create_topics.sh
│   ├── create_real_topic.sh
│   ├── prepare_real_references.py
│   ├── run_demo.sh                   # Démonstration de simulation
│   ├── run_pipeline.sh               # Parcours standard de simulation
│   ├── run_real_pipeline.sh          # Traitement réel jusqu’à PostgreSQL, sans nouvel envoi
│   ├── simulate_trip_updates.py
│   └── snapshot_resume_state.py      # Captures simulation/réel via --mode
├── spark/
│   ├── requirements.txt
│   ├── resources/simulation/
│   ├── src/jobs/
│   ├── src/transforms/
│   └── tests/
└── data/
    ├── lakehouse/                    # Delta de simulation
    ├── checkpoints/                  # Checkpoints de simulation
    ├── lakehouse_real/               # Delta réel
    ├── checkpoints_real/             # Checkpoints réels
    ├── reference/real/sncf/           # Références réelles et manifeste
    ├── source_checks/                # Instantanés et exports locaux
    └── validation/                   # Captures de contrôle de reprise
```

Les données générées, archives GTFS, instantanés, exports JSONL et checkpoints ne sont pas nécessaires dans Git. Les scripts, contrats, tests et procédures doivent permettre de les recréer.

Le classeur Tableau est enregistré localement ; son ajout au dépôt reste conditionné à une vérification des secrets et informations de connexion. L’arborescence indique son emplacement prévu, pas une preuve qu’il est déjà versionné.

## 2. Stack et modèle de données

### Technologies

| Composant | Technologie | Rôle |
|---|---|---|
| Infrastructure | Docker Compose | Services locaux et volumes persistants |
| Bus d'événements | Apache Kafka 4.1.0, KRaft | Transport des messages |
| Inspection Kafka | Kafka UI | Consultation des topics et messages |
| Ingestion et normalisation | Java 17, Maven, Jackson | Simulation, décodage GTFS-RT, mapping et publication JSON |
| Formats de transport | GTFS-RT Protobuf et GTFS statique | Mises à jour de trajets et référentiels réels |
| Lecture Java | gtfs-realtime-bindings 0.0.8, Commons CSV 1.13.0 | Décodage Protobuf et lecture des CSV GTFS |
| Simulateur complémentaire | Python, confluent-kafka | Génération de volumes et de cas invalides |
| Traitements | PySpark 3.5.3 | Ingestion, transformations et agrégations |
| Stockage analytique | Delta Lake 3.2.0 | Couches Bronze / Silver / Gold |
| Restitution | PostgreSQL 17, psycopg | Publication des indicateurs |
| Visualisation principale | Tableau Desktop Free Edition | Deux dashboards locaux, connexion PostgreSQL en direct |
| BI historique | Metabase | Dashboards historiques ; service encore présent dans Compose |
| Tests | JUnit 5, pytest | Vérification du producteur et des transformations |

Kafka, Kafka UI et PostgreSQL tournent dans Docker. Le service Metabase historique reste dans le Compose fourni. Le producteur Java, les jobs Spark et Tableau Desktop s’exécutent sur le Mac ; Spark utilise le mode `local[*]`. Tableau n’est pas un service Docker.

### Couches du lakehouse

| Couche | Table / dossier | Contenu |
|---|---|---|
| Bronze | `bronze/trip_updates` | Payload JSON original, clé Kafka, topic, partition, offset et timestamps |
| Silver | `silver/trip_delays` | Événements typés, retards en minutes et noms de lignes / arrêts |
| Silver | `silver/rejected_events` | Payloads rejetés, métadonnées Kafka et motif de validation |
| Gold | `gold/delay_by_line_15min` | Agrégats par ligne et fenêtre de 15 minutes |
| Gold | `gold/station_delay_daily` | Agrégats quotidiens par arrêt |

Les mêmes structures analytiques sont utilisées dans deux périmètres :

| Composant | Simulation | Réel |
|---|---|---|
| Topic | `sncf.trip_updates.raw` | `sncf.trip_updates.real.raw` |
| Racine Delta | `data/lakehouse` | `data/lakehouse_real` |
| Checkpoints | `data/checkpoints` | `data/checkpoints_real` |
| Références | `spark/resources/simulation` | `data/reference/real/<lot>` ; historique : `data/reference/real/sncf` |
| Schéma PostgreSQL | `public` | `sncf_real` |

Tables PostgreSQL dans chacun des schémas :
- `gold_delay_by_line_15min`.
- `gold_station_delay_daily`.

Cette isolation est logique ; les deux schémas utilisent actuellement la même base et le même utilisateur PostgreSQL. Le publisher refuse les couples lakehouse/schéma non autorisés.

### Contrat et règles métier

Le format JSON est décrit dans [le contrat de données](docs/data-contract.md), actualisé pour la simulation et le mapping réel : timestamps, horaires reconstitués, avances autorisées, identité des observations et séparation entre exigences du contrat et contrôles effectivement implémentés. Le runbook documente les commandes validées et les procédures de reprise ; le mode quasi-temps réel n’y est pas encore une procédure exécutable.

Règles communes :

- Clé Kafka : `trip_id`.
- Identifiant de déduplication : `event_id`.
- Conversion : `delay_minutes = delay_seconds / 60.0`.
- Retard significatif : `delay_seconds > 300`, soit strictement plus de cinq minutes.
- Silver applique un watermark d'un jour et `dropDuplicatesWithinWatermark` sur `event_id`.
- Les noms sont enrichis depuis les références correspondant au parcours choisi.

Mapping réel actuel :

- `source = sncf_gtfs_rt`, `event_type = trip_update`, `schema_version = 1`.
- Un événement par mise à jour d'arrêt retenue.
- Seules les courses et mises à jour d'arrêt `SCHEDULED` sont retenues.
- Les courses `ADDED` et `CANCELED`, ainsi que les arrêts `SKIPPED`, sont exclus du premier périmètre analytique et comptabilisés.
- Priorité à l'arrivée si elle contient explicitement `time` et `delay` ; sinon, sélection du départ s'il contient ces deux champs.
- `estimated_timestamp` vient du `time` sélectionné.
- `scheduled_timestamp` est reconstitué par `time - delay`, et non lu dans `stop_times.txt`.
- `event_timestamp` correspond à la publication de l'instantané GTFS-RT.
- `ingested_at` est conservé depuis l'export JSONL lors du rejeu ; il ne représente donc pas nécessairement l'instant exact d'envoi Kafka. Bronze ajoute son propre timestamp d'ingestion.
- Le mapper préserve les retards négatifs lorsqu'ils sont présents.
- `event_id` est un UUID déterministe incluant l'observation et ses caractéristiques, mais pas `ingested_at`. Une nouvelle publication de flux avec un autre timestamp représente une nouvelle observation.

Les compteurs d'événements ne sont pas des nombres de trains uniques. `delayed_trip_count` compte les courses distinctes ayant au moins une observation en retard significatif dans la fenêtre ; `delayed_event_count` compte les observations en retard significatif par arrêt et par jour.

Les fenêtres et jours du parcours réel sont fondés sur le timestamp d’observation du flux, pas sur les heures de passage. Les moyennes portent sur les observations conservées, et non sur une mesure globale de ponctualité.

La somme de `delayed_trip_count` sur plusieurs fenêtres n’est pas un nombre de trains uniques sur toute la période. La table par arrêt ne contient pas `route_id` : aucune navigation ligne → arrêts n’est annoncée sans adapter cet agrégat. Les deux tables de restitution sont utilisées comme des sources Tableau distinctes.

## 3. Installation et démonstration en simulation

### Prérequis

- Docker et Docker Compose.
- JDK 17 et Maven.
- Python 3.12, version utilisée pour les validations locales et la CI.
- Bash, notamment sur macOS ou Linux.
- Pour consulter les dashboards réels : Tableau Desktop Free Edition sur Mac et le pilote PostgreSQL demandé par Tableau.
- Accès Internet à la première installation pour télécharger les images et dépendances.

Exécuter les commandes suivantes à la racine du dépôt.

### Étape 1 — Configurer l'environnement

```bash
if [ ! -f .env ]; then
  cp .env.example .env
fi
```

Ne pas écraser un `.env` existant.

Modifier le mot de passe PostgreSQL dans `.env`, puis charger les variables :

```bash
set -a
source .env
set +a

python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r spark/requirements.txt
python -m pip install -r requirements-simulator.txt
```

Le script `run_pipeline.sh` utilise actuellement PostgreSQL sur `localhost:5432`. Conserver `POSTGRES_PORT=5432` pour cette procédure. Le topic créé par `create_topics.sh` est également fixé à `sncf.trip_updates.raw`.

### Étape 2 — Démarrer les services

Sélectionner explicitement l'installation originale :

```bash
export COMPOSE_PROJECT_NAME=sncf-realtime-lakehouse
pwd
printf 'Projet Compose : %s\n' "$COMPOSE_PROJECT_NAME"
docker compose up -d kafka kafka-ui postgres
docker compose ps
```

Cette commande démarre uniquement l’infrastructure utile au parcours principal, sans imposer Metabase. Un simple `docker compose up -d` démarre également le service Metabase tant qu’il figure dans Compose.

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
(
  unset LAKEHOUSE_ROOT CHECKPOINT_ROOT GTFS_REFERENCE_ROOT
  unset BRONZE_TRIP_UPDATES_PATH BRONZE_TRIP_UPDATES_CHECKPOINT_PATH
  unset POSTGRES_TARGET_SCHEMA

  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export KAFKA_TRIP_UPDATES_TOPIC=sncf.trip_updates.raw

  bash scripts/run_demo.sh
)
```

Ce script vérifie le topic, publie un événement simulé avec Java, puis lance Bronze, Silver, les contrôles qualité Silver, les deux agrégations Gold et la publication PostgreSQL.

Chaque lancement produit un nouvel événement. Pour vérifier une reprise sans nouvel envoi, utiliser uniquement `bash scripts/run_pipeline.sh`.

Il suppose que les services, le topic, les tables PostgreSQL et l'environnement `.venv` sont déjà prêts.

Les autres commandes de cette section supposent également les valeurs de simulation : ne pas conserver les variables du parcours réel exportées globalement dans le terminal.

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

Kafka UI permet d’inspecter les messages. Pour vérifier les agrégats de simulation :

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -X -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT * FROM public.gold_delay_by_line_15min ORDER BY window_start, route_id;
SELECT * FROM public.gold_station_delay_daily ORDER BY event_date, stop_id;
SQL
```

Les dashboards Tableau montrés en tête du README utilisent `sncf_real`, pas `public`. Lancer une démonstration de simulation ne doit pas modifier leur source. Une restitution Tableau de simulation peut être créée séparément, mais n’est pas déclarée validée ici.

### Dashboard de simulation — historique Metabase

Le dashboard historique « Suivi des retards ferroviaire (simulation) » présente le retard moyen par ligne et par gare à partir de `public.gold_delay_by_line_15min` et `public.gold_station_delay_daily`.

Sa procédure reste dans [la documentation historique Metabase](docs/metabase.md). Les anciennes captures éventuelles ne sont pas une preuve de fonctionnement de Tableau ni du parcours réel. Les données simulées ne représentent pas la ponctualité SNCF.

## 4. Parcours réel SNCF — Exécution à la demande

### État validé et prérequis

Le premier test réel portait sur 10 observations du 8 octobre. Le 17 octobre 2026, un rejeu contrôlé de 100 observations de l’instantané du 15 octobre a porté le total à 110. La publication PostgreSQL, les volumes SQL, l’actualisation Tableau et le fonctionnement du filtre de période ont été vérifiés. Les résultats du premier test restent conservés ci-dessous comme historique.

Le téléchargement HTTP existe dans une classe Java dédiée et un script de collecte/préparation. Le traitement après publication Kafka est regroupé dans `scripts/run_real_pipeline.sh`. Il n’existe pas encore de commande unique validée qui enchaîne collecte, publication Kafka, Spark, PostgreSQL et actualisation Tableau. Les étapes détaillées ci-dessous conservent la procédure explicite ; l’alternative HTTP et les résultats des deux lots sont également documentés.

Cette procédure suppose que l'installation de simulation a déjà initialisé l'infrastructure, les dépendances et les tables `public`. Le script SQL réel copie leur structure.

Exécuter les commandes depuis la racine du dépôt original. Ne pas modifier le topic par défaut de `.env` et ne pas lancer `run_demo.sh` ou `run_pipeline.sh` pour orchestrer le parcours réel.

### Commande de traitement réel validée

Après préparation des références et publication contrôlée des événements dans Kafka, exécuter depuis la racine du dépôt :

```bash
# Exemple du lot utilisé pour la validation du 17 octobre.
# Ce dossier doit déjà contenir les références préparées pour ce lot.
bash scripts/run_real_pipeline.sh \
  data/reference/real/collection-eYyLYD
```

Pour une nouvelle collecte, remplacer l’argument par le dossier de références affiché lors de sa préparation. Ne pas créer un dossier vide pour reproduire le nom de cet exemple.

Le script enchaîne Bronze, Silver, les contrôles Silver, les deux agrégations Gold et la publication dans `sncf_real`, en utilisant `data/lakehouse_real` et les checkpoints réels existants. Il ne télécharge aucun flux, ne publie aucun message Kafka et n’actualise pas Tableau. Ne pas supprimer les checkpoints pour une relance.

Cette commande remplace l’exécution manuelle des étapes 6 et 7 ci-dessous ; ne pas effectuer les deux procédures pour un même contrôle. Elle a été exécutée avec succès dans l’installation locale. Une reprise normale réelle a ensuite été vérifiée par comparaison avant/après le 20 octobre (section 5). Le parcours réel depuis un nouveau clone et la reprise du futur mode actif restent à valider séparément.

### Étape 1 — Télécharger les sources

```bash
export COMPOSE_PROJECT_NAME=sncf-realtime-lakehouse

mkdir -p data/source_checks
SOURCE_DIR="$(mktemp -d data/source_checks/sncf-XXXXXX)"

REALTIME_FILE="$PWD/$SOURCE_DIR/trip_updates.pb"
GTFS_ZIP="$PWD/$SOURCE_DIR/sncf_gtfs.zip"

curl --fail --location --silent --show-error \
  --connect-timeout 10 --max-time 60 \
  --output "$REALTIME_FILE" \
  'https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates'

curl --fail --location --silent --show-error \
  --connect-timeout 10 --max-time 180 \
  --output "$GTFS_ZIP" \
  'https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip'
```

Si un téléchargement échoue, arrêter la procédure. Les fichiers téléchargés peuvent être incomplets et ne doivent pas être utilisés comme des sources validées.

Chaque exécution crée un nouveau dossier. Les effectifs des sources évoluent : les résultats historiques ci-dessous ne sont pas des valeurs fixes attendues pour tout nouvel instantané.

### Étape 2 — Inspecter et exporter avec Java

```bash
JSONL_OUTPUT="$PWD/$SOURCE_DIR/trip_updates.canonical.jsonl"

(
  set -e
  cd producer-java

  mvn --batch-mode --no-transfer-progress test

  mvn --batch-mode --no-transfer-progress compile exec:java \
    -Dexec.mainClass=fr.remyqueny.sncf.producer.GtfsRealtimeInspector \
    -Dexec.args="\"$REALTIME_FILE\""

  mvn --batch-mode --no-transfer-progress exec:java \
    -Dexec.mainClass=fr.remyqueny.sncf.producer.GtfsStaticReference \
    -Dexec.args="\"$GTFS_ZIP\""

  mvn --batch-mode --no-transfer-progress exec:java \
    -Dexec.mainClass=fr.remyqueny.sncf.producer.GtfsRealtimeEnrichmentInspector \
    -Dexec.args="\"$REALTIME_FILE\" \"$GTFS_ZIP\""

  mvn --batch-mode --no-transfer-progress exec:java \
    -Dexec.mainClass=fr.remyqueny.sncf.producer.GtfsRealtimeJsonExporter \
    -Dexec.args="\"$REALTIME_FILE\" \"$GTFS_ZIP\" \"$JSONL_OUTPUT\""
)
```

Vérifier les compteurs de résolution et d'exclusion avant de publier. L'inspecteur affiche deux exemples ; l'exporter convertit tous les candidats acceptés et vérifie l'unicité des identifiants.

L'exporter refuse d'écraser un fichier existant. Il écrit une sortie `.partial`, renommée seulement après réussite. Un `.partial` laissé après un échec n'est pas un export valide.

### Étape 3 — Préparer les références réelles

Pour conserver la provenance de chaque préparation, utiliser un dossier correspondant au téléchargement :

```bash
REFERENCE_DIR="$PWD/data/reference/real/$(basename "$SOURCE_DIR")"

.venv/bin/python scripts/prepare_real_references.py \
  "$GTFS_ZIP" "$REFERENCE_DIR"
```

Le script génère `routes.csv`, `stops.csv` et `manifest.json`. Il refuse les identifiants dupliqués, les noms absents et un dossier cible déjà existant.

Le nom de ligne utilise `route_long_name`, ou `route_short_name` si le nom long est vide. Le manifeste conserve l'empreinte SHA-256 de l'archive et la date de préparation.

Le test historique a utilisé `data/reference/real/sncf`. Le job Silver accepte également le dossier spécifique défini ici via `GTFS_REFERENCE_ROOT`.

### Étape 4 — Initialiser le topic et les tables réelles

```bash
bash scripts/create_real_topic.sh

docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -X -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < infra/postgres/002_real_gold_tables.sql
```

Le topic réel utilise une partition et un facteur de réplication de 1. Les tables du schéma `sncf_real` reprennent la structure des tables `public`, sans copier leurs données.

### Étape 5 — Publier un petit échantillon

Vérifier que l'export contient au moins 10 lignes avant cette commande :

```bash
wc -l "$JSONL_OUTPUT"

(
  cd producer-java

  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  mvn --batch-mode --no-transfer-progress exec:java \
    -Dexec.mainClass=fr.remyqueny.sncf.producer.RealJsonlProducer \
    -Dexec.args="\"$JSONL_OUTPUT\" 10"
)
```

`RealJsonlProducer` valide la sélection avant l'envoi, utilise `trip_id` comme clé et cible exclusivement `sncf.trip_updates.real.raw`. Le nombre demandé doit être compris entre 1 et 20 000 et ne pas dépasser les lignes disponibles.

Ne pas répéter cette commande pour une simple vérification : elle republie les mêmes événements. Si certains messages ont été livrés avant un échec, diagnostiquer la reprise avant un nouvel envoi. L'idempotence du producteur Kafka ne déduplique pas automatiquement deux exécutions distinctes.

### Étape 6 — Exécuter Bronze, Silver, les contrôles et Gold

La validation historique a utilisé les racines dédiées ci-dessous. Ces commandes ne suppriment aucun checkpoint et ne réinitialisent aucune table.

```bash
(
  set -e

  export PYTHONPATH="$PWD/spark/src"
  export PYSPARK_PYTHON="$PWD/.venv/bin/python"
  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export KAFKA_TRIP_UPDATES_TOPIC=sncf.trip_updates.real.raw

  export LAKEHOUSE_ROOT="$PWD/data/lakehouse_real"
  export CHECKPOINT_ROOT="$PWD/data/checkpoints_real"
  export GTFS_REFERENCE_ROOT="$REFERENCE_DIR"

  export BRONZE_TRIP_UPDATES_PATH="$LAKEHOUSE_ROOT/bronze/trip_updates"
  export BRONZE_TRIP_UPDATES_CHECKPOINT_PATH="$CHECKPOINT_ROOT/bronze_trip_updates"

  .venv/bin/python spark/src/jobs/stream_bronze.py
  .venv/bin/python spark/src/jobs/stream_silver.py
  .venv/bin/python spark/src/jobs/inspect_silver.py
  .venv/bin/python spark/src/jobs/build_gold_delay_by_line.py
  .venv/bin/python spark/src/jobs/build_gold_station_delay_daily.py
)
```

Les variables restent limitées au sous-shell. Ne pas modifier le topic consommé en réutilisant un checkpoint de simulation.

Un nouveau référentiel ne réenrichit pas automatiquement les événements déjà écrits dans Silver. Les traitements réels restent cumulatifs : le total validé est maintenant de 110 observations, et les prochaines publications peuvent l’augmenter.

### Étape 7 — Publier Gold dans sncf_real

```bash
(
  set -e

  export COMPOSE_PROJECT_NAME=sncf-realtime-lakehouse
  export PYTHONPATH="$PWD/spark/src"
  export PYSPARK_PYTHON="$PWD/.venv/bin/python"
  export LAKEHOUSE_ROOT="$PWD/data/lakehouse_real"
  export POSTGRES_TARGET_SCHEMA=sncf_real
  export POSTGRES_HOST=localhost
  export POSTGRES_PORT=5432

  POSTGRES_USER="$(docker compose exec -T postgres sh -c \
    'printf "%s" "$POSTGRES_USER"')"
  POSTGRES_DB="$(docker compose exec -T postgres sh -c \
    'printf "%s" "$POSTGRES_DB"')"
  POSTGRES_PASSWORD="$(docker compose exec -T postgres sh -c \
    'printf "%s" "$POSTGRES_PASSWORD"')"

  export POSTGRES_USER POSTGRES_DB POSTGRES_PASSWORD

  .venv/bin/python spark/src/jobs/publish_postgres.py
)
```

Vérifier dans les logs la racine `data/lakehouse_real/gold` et le schéma `sncf_real`. Ne pas partager les identifiants ou le mot de passe.

Pour contrôler les données publiées :

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -X -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT window_start, window_end, line_name, event_count,
       delayed_trip_count, average_delay_minutes, max_delay_minutes
FROM sncf_real.gold_delay_by_line_15min
ORDER BY window_start, route_id;

SELECT COUNT(*) AS aggregate_count,
       SUM(event_count) AS total_events,
       SUM(delayed_event_count) AS significant_delay_events
FROM sncf_real.gold_station_delay_daily;
SQL
```

### Validation complémentaire exécutée le 17 octobre 2026

#### Profil du nouveau lot avant publication

Les 100 premières observations de `data/source_checks/collection-eYyLYD/trip_updates.canonical.jsonl` ont été examinées avant leur envoi. Le dossier de références associé est `data/reference/real/collection-eYyLYD`. Ces noms désignent des fichiers locaux de cette exécution, non fournis par un nouveau clone.

| Contrôle de l’échantillon | Résultat |
|---|---:|
| Date d’observation | 2026-10-15 |
| Observations sélectionnées | 100 |
| Identifiants `event_id` distincts | 100 |
| Courses `trip_id` distinctes | 8 |
| Lignes `route_id` distinctes | 6 |
| Arrêts `stop_id` distincts | 74 |
| Observations avec retard strictement supérieur à 5 minutes | 39 |
| Observations avec avance | 0 |

La source de ces 100 observations est exclusivement `sncf_gtfs_rt`. Les 39 observations en retard significatif ne sont pas un décompte de 39 trains distincts.

#### Publication et traitement observés

- Une publication de 100 événements dans `sncf.trip_updates.real.raw`.
- Accusés de réception sur la partition `0`, offsets `10` à `109`.
- Bilan du producteur : `status=completed ... delivered_count=100`, puis `BUILD SUCCESS`.
- Exécution réussie de `run_real_pipeline.sh` avec les références du lot.
- Publication de 7 agrégats par ligne/fenêtre et de 84 agrégats par arrêt/jour dans `sncf_real`.

Ne pas republier ce JSONL pour retrouver ces nombres. Les offsets ci-dessus sont ceux de cette exécution, pas des valeurs attendues pour toute installation. Les compteurs détaillés de rejets et de doublons Silver du nouveau lot restent à archiver ; aucun résultat nul n’est déduit du seul succès de la publication.

#### Résultats PostgreSQL vérifiés

| Date d’observation | Agrégats arrêt × jour | Observations | Observations > 5 min | Part > 5 min |
|---|---:|---:|---:|---:|
| 2026-10-08 | 10 | 10 | 0 | 0,00 % |
| 2026-10-15 | 74 | 100 | 39 | 39,00 % |
| Total des deux lots | 84 | 110 | 39 | 35,45 % |

La table `sncf_real.gold_delay_by_line_15min` contient 7 agrégats et représente également 110 observations. Les deux tables décrivent les mêmes observations à des granularités différentes : leurs volumes ne doivent pas être additionnés. Les 84 agrégats arrêt/jour ne signifient pas 84 arrêts distincts sur toute la période ; les 7 agrégats ligne/fenêtre ne signifient pas 7 lignes distinctes.

#### Résultat Tableau observé

Après actualisation manuelle, la synthèse affiche 110 observations, un retard moyen pondéré de 7,68 minutes et un retard maximal de 45 minutes. Ces deux valeurs de retard sont celles affichées dans Tableau ; les requêtes SQL exécutées dans cette validation ont contrôlé les volumes et les observations au-delà du seuil.

Les descriptions mentionnent les instantanés des 8 et 15 octobre ; les moyennes sont affichées avec deux décimales et les pourcentages par arrêt avec une décimale. Le fonctionnement du filtre de période a été confirmé manuellement. Les captures de travail montrent les nouvelles valeurs ; leur export propre et leur archivage dans le dépôt restent à finaliser.

### Résultats du test historique du 8 octobre 2026

| Contrôle | Résultat |
|---|---:|
| Courses dans le GTFS-RT | 2 001 |
| Mises à jour d'arrêt dans le GTFS-RT | 17 869 |
| Courses SCHEDULED enrichies | 1 776 |
| Courses ADDED exclues | 196 |
| Courses CANCELED exclues | 29 |
| Arrêts exclus avec ces courses | 838 |
| Arrêts SKIPPED exclus | 53 |
| Événements JSONL exportés et identifiants distincts | 16 978 |
| Événements publiés et présents dans Bronze / Silver | 10 |
| Rejets Silver / noms non résolus sur l'échantillon | 0 |
| Agrégats Gold / PostgreSQL par ligne | 1 |
| Agrégats Gold / PostgreSQL par arrêt | 10 |

Le bilan de conversion est : `17 869 = 838 + 53 + 16 978`.

Pour les 10 observations de la course sélectionnée :

- Ligne : `Lux/Alsace/Loraine - Paca`.
- Timestamp du flux : `2026-10-08T17:18:01Z`.
- Fenêtre UTC : `17:15:00` à `17:30:00`.
- Retard moyen : 1,5 minute ; maximal : 5 minutes.
- Colmar, Mulhouse et Belfort - Montbéliard TGV : 5 minutes.
- Les sept autres arrêts : 0 minute.
- Compteurs de retard strictement supérieur à 5 minutes : 0.

Ces résultats sont ceux d'un échantillon validé, pas une prévision du contenu des futurs flux.

### Collecte HTTP Java — composant disponible

`GtfsRealtimeHttpDownloader` utilise le client HTTP Java avec délai de connexion de 10 secondes et délai de requête de 60 secondes. Il contrôle le statut HTTP, la taille du fichier après réception, le décodage Protobuf et la présence d’un timestamp de publication. Il refuse d’écraser une destination existante et nettoie le fichier temporaire après échec.

Le script `collect_real_snapshot.sh` prend une archive GTFS statique déjà téléchargée :

```bash
# Remplacer le chemin par une archive GTFS statique locale existante.
GTFS_ARCHIVE="/chemin/vers/referentiel_gtfs.zip"
bash scripts/collect_real_snapshot.sh "$GTFS_ARCHIVE"
```

Il prépare les références et leur manifeste, télécharge le GTFS-RT via Java, puis génère un export JSONL non vide. Il affiche les chemins de l’instantané, de l’export et des références. Chaque exécution crée un nouveau dossier de collecte.

Ce script s’arrête avant Kafka et Spark. Pour utiliser ses sorties, reprendre les chemins affichés comme `REALTIME_FILE`, `JSONL_OUTPUT` et `REFERENCE_DIR`, contrôler un échantillon puis effectuer une seule publication Kafka. Lancer ensuite `bash scripts/run_real_pipeline.sh "$REFERENCE_DIR"`. Ne pas relancer la préparation dans un dossier déjà créé. La validation complémentaire porte sur 100 observations sélectionnées, pas sur la totalité d’un export ni sur une collecte permanente.

### Restitution réelle dans Tableau

Les deux dashboards visibles en tête du README ont été construits dans Tableau Desktop Free Edition sur Mac, en connexion directe à PostgreSQL. La synthèse et l’analyse détaillée correspondent à deux pages du même classeur local.

#### Connexion et sources

1. Démarrer Docker Desktop puis PostgreSQL :

```bash
export COMPOSE_PROJECT_NAME=sncf-realtime-lakehouse
docker compose up -d postgres
docker compose ps postgres
```

2. Dans Tableau, sélectionner PostgreSQL. Installer le pilote demandé et redémarrer Tableau si nécessaire.
3. Se connecter à `localhost`, avec le port publié dans `.env` (`5432` dans les validations locales), et les valeurs de `POSTGRES_DB`, `POSTGRES_USER` et `POSTGRES_PASSWORD`. Ne pas partager le mot de passe.
4. Utiliser deux sources séparées, en mode « En direct », avec ces requêtes :

```sql
SELECT * FROM sncf_real.gold_delay_by_line_15min
```

```sql
SELECT * FROM sncf_real.gold_station_delay_daily
```

Créer la seconde via « Données → Nouvelle source de données », pas en ajoutant une relation entre les deux tables. Les granularités sont différentes : ligne × fenêtre de 15 minutes d’un côté, arrêt × jour de l’autre.

#### Indicateurs et calculs

| Vue | Indicateur | Calcul |
|---|---|---|
| Synthèse | Observations analysées | `SUM([event_count])` sur la source par ligne |
| Synthèse et analyses | Retard moyen pondéré | Somme des moyennes multipliées par les effectifs, divisée par le total d’observations |
| Synthèse et analyses | Retard maximal | `MAX([max_delay_minutes])` |
| Analyse des arrêts | Part des observations > 5 min | `SUM([delayed_event_count]) / SUM([event_count])` |

Calcul pondéré dans chaque source concernée :

```text
IF SUM([event_count]) > 0 THEN
    SUM([average_delay_minutes] * [event_count])
    / SUM([event_count])
END
```

Part des observations au-delà du seuil, sur la source par arrêt :

```text
IF SUM([event_count]) > 0 THEN
    SUM([delayed_event_count]) / SUM([event_count])
END
```

Formater le ratio en pourcentage, sans multiplier la formule par 100. Les vues validées affichent les effectifs sans décimales, les retards moyens avec deux décimales et les pourcentages avec une décimale. Ne pas sommer les moyennes. Les moyennes Gold étant arrondies à deux décimales, leur recomposition peut être légèrement approximative.

#### Filtre de période

Le dashboard d’analyse des arrêts utilise une plage de dates sur `event_date`. Son fonctionnement a été confirmé manuellement après ajout du lot du 15 octobre. Le filtre reste propre à l’analyse des arrêts et ne doit pas être présenté comme un filtre global du dashboard de synthèse.

Références pour les contrôles sur une date isolée :

| Période sélectionnée | Observations SQL | Observations > 5 min | Part > 5 min |
|---|---:|---:|---:|
| 08/10/2026 uniquement | 10 | 0 | 0 % |
| 15/10/2026 uniquement | 100 | 39 | 39 % |

Avec les deux lots présents, exclure le 8 octobre ne suffit plus à obtenir une vue vide : les données du 15 octobre restent affichables. Une période excluant les deux dates doit ne montrer aucune observation. Pour la présentation d’ensemble, utiliser une plage du 8 au 15 octobre et préciser qu’elle contient seulement deux instantanés, sans couverture continue entre les deux dates.

Deux dates sont maintenant disponibles, mais aucune courbe temporelle n’a été construite ou validée. Les deux échantillons sélectionnés ne suffisent pas à démontrer une tendance représentative.

#### Contrôles SQL

Pour ouvrir une console PostgreSQL depuis le terminal, sans installer `psql` sur le Mac :

```bash
docker exec -it sncf-postgres sh -c 'exec psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}"'
```

Coller les requêtes SQL à l’invite `psql`, et non directement à l’invite du shell. Quitter la console avec `\q`. Cette commande utilise le nom de conteneur `sncf-postgres` constaté dans l’installation locale.

Contrôles exécutés sur les données publiées :

```sql
SELECT
    event_date,
    COUNT(*) AS station_day_rows,
    SUM(event_count) AS observations,
    SUM(delayed_event_count) AS observations_over_5min,
    ROUND(
        100.0 * SUM(delayed_event_count)
        / NULLIF(SUM(event_count), 0),
        2
    ) AS observations_over_5min_pct
FROM sncf_real.gold_station_delay_daily
GROUP BY event_date
ORDER BY event_date;

SELECT
    COUNT(*) AS line_window_rows,
    SUM(event_count) AS observations
FROM sncf_real.gold_delay_by_line_15min;
```

Résultats obtenus dans l’installation validée : `(2026-10-08, 10, 10, 0, 0.00)` et `(2026-10-15, 74, 100, 39, 39.00)` pour la première requête ; `(7, 110)` pour la seconde. De nouvelles publications peuvent modifier ces résultats.

Contrôle complémentaire du bandeau de synthèse, à exécuter pour vérifier aussi les retards moyen et maximal côté SQL :

```sql
SELECT
    SUM(event_count) AS observations,
    ROUND(
        (SUM(average_delay_minutes * event_count)
         / NULLIF(SUM(event_count), 0))::numeric, 2
    ) AS retard_moyen_minutes,
    MAX(max_delay_minutes) AS retard_maximal_minutes
FROM sncf_real.gold_delay_by_line_15min;
```

Contrôle du tableau par arrêt, sur la même période que le filtre Tableau :

```sql
SELECT
    stop_id,
    stop_name,
    SUM(event_count) AS observations,
    ROUND(
        (SUM(average_delay_minutes * event_count)
         / NULLIF(SUM(event_count), 0))::numeric, 2
    ) AS retard_moyen_minutes,
    ROUND(
        100.0 * SUM(delayed_event_count)
        / NULLIF(SUM(event_count), 0), 1
    ) AS observations_plus_de_5_min_pct,
    MAX(max_delay_minutes) AS retard_maximal_minutes
FROM sncf_real.gold_station_delay_daily
WHERE event_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY stop_id, stop_name
ORDER BY retard_moyen_minutes DESC, stop_name;
```

Le SQL ci-dessus multiplie par 100 pour retourner un nombre entre 0 et 100 ; le calcul Tableau retourne un ratio entre 0 et 1 affiché avec le format pourcentage. Les résultats représentent le même indicateur.

#### Ouverture et actualisation

Le classeur local porte le nom `sncf_real_dashboard`. Un fichier `.twb` en connexion directe conserve les vues et calculs, pas une copie autonome de PostgreSQL. Son versionnement reste à contrôler ; les captures ne garantissent pas sa présence dans un nouveau clone.

Pour consulter de nouveau les vues : démarrer PostgreSQL, ouvrir le classeur, renseigner la connexion si demandé et actualiser les données. Consulter [le guide Tableau](bi/tableau/README.md) pour la reconstruction et le dépannage.

Actualiser Tableau ne télécharge pas les sources SNCF et ne déclenche pas le pipeline. Le 17 octobre, le scénario « nouvelle publication Kafka → traitement → PostgreSQL → actualisation Tableau → nouvelles valeurs » a été exécuté : le KPI est passé de 10 à 110 observations. Les contrôles SQL, les captures de travail et le test manuel du filtre documentent cette validation ; les exports propres et leur archivage dans le dépôt restent à finaliser. Une connexion en direct n’implique pas une actualisation visuelle automatique en continu.

## 5. Tests et fiabilité

### Tests Java et Python

```bash
(cd producer-java && mvn test)

PYTHONPATH="$PWD/spark/src" \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest spark/tests -v
```

Les suites couvrent notamment la sérialisation JSON, la cohérence des événements Java, la couverture des références simulées, les transformations et agrégations Python ainsi que les contrôles qualité Silver.

Huit tests ont été ajoutés pour `GtfsStopEventMapper` : priorité à l'arrivée, repli sur le départ avec retard zéro explicite, absence de retard explicite, retard négatif, stabilité de l'identifiant lorsque seule l'ingestion change, nouvel identifiant pour un autre instantané, exclusions métier et absence de ligne.

Les compteurs de 12 tests Java et 40 cas Python appartiennent aux validations historiques. Les suites Java et Python ont été confirmées réussies le 20 octobre, sans nouveau total précis communiqué. Un comptage statique de méthodes Java `@Test` ne remplace pas un rapport d’exécution.

Le 27 octobre, le contrôle `check_enrichment` a été ajouté à `inspect_silver.py`, après les contrôles existants. Il vérifie que `route_id`, `stop_id`, `line_name` et `stop_name` sont non nuls et non vides après `trim`. Ses nouveaux tests utilisent des DataFrames en mémoire :

- Une ligne complète acceptée.
- Douze cas de valeur manquante : quatre champs × `null`, chaîne vide, espaces.
- Un mélange de lignes complètes et incomplètes refusé.

Résultat ciblé observé : `14 passed, 38 deselected in 8.95s`. Les 38 cas désélectionnés étaient exclus par le filtre, pas en échec. La suite Python complète puis l’inspection Silver réelle ont ensuite été confirmées sans erreur, sans nouveau total de tests ou de lignes communiqué.

Pour reproduire uniquement les cas ciblés :

```bash
PYTHONPATH="$PWD/spark/src" \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest \
  spark/tests/test_trip_updates.py \
  -k "enrichment_check" -v
```

Exécuter ensuite la suite Python complète. Consulter le rapport Maven, le bilan pytest et la CI du commit courant pour les nombres effectivement exécutés. Les cas paramétrés sont comptés séparément ; un nombre de cas n’est pas un nombre de fonctions.

### Intégration continue

La CI est définie dans `.github/workflows/ci.yml`. Elle exécute les tests Java et Python/Spark lors des push et des pull requests, et peut être lancée manuellement.

La CI existe déjà et les deux jobs ont été exécutés avec succès, avec Java 17 et Python 3.12 sur un runner Linux. Consulter l'onglet Actions pour le résultat du commit courant ; une réussite locale ne prouve pas à elle seule celle de ce commit sur GitHub.

Le workflow ne démarre pas l’infrastructure Kafka/PostgreSQL et n’ouvre pas Tableau : il ne valide ni le pipeline de bout en bout ni les dashboards. Il n’inclut pas encore de lint bloquant.

### Logs du producteur Java

Le producteur utilise `slf4j-api` et `slf4j-simple` en version `1.7.36`. Le fichier de propriétés SLF4J présent dans le dépôt doit être vérifié séparément ; le bilan ci-dessous porte sur les logs observés pendant les exécutions.

- Les messages applicatifs sont écrits au niveau `INFO` pour les succès et `ERROR` pour les échecs.
- Le succès est journalisé après l'accusé de réception Kafka, avec `event_id`, topic, partition et offset.
- Les erreurs incluent le type d'exception, le message et la trace de l'exception.
- Les logs applicatifs utilisent un format texte `clé=valeur`, pas un format JSON.

Le chemin d'erreur du producteur initial a été vérifié avec `SIMULATION_MODE=false`, qui reste non implémenté dans `ProducerApplication` et échoue avant tout envoi Kafka.

Le parcours réel utilise des classes distinctes : téléchargement éventuel par `GtfsRealtimeHttpDownloader`, normalisation avec `GtfsRealtimeJsonExporter`, puis publication par `RealJsonlProducer`, et non ce flag. Le publisher journalise la validation préalable, les accusés de réception Kafka et le nombre livré. Il ne télécharge pas lui-même la source HTTP.

### Contrôle des événements invalides

```bash
python scripts/simulate_trip_updates.py --count 10 --invalid
bash scripts/run_pipeline.sh
```

Le simulateur injecte des cas avec `trip_id` absent, retard non numérique ou timestamp invalide. Silver conserve les rejets dans `silver/rejected_events` avec leur payload et un motif d'erreur.

Avant les calculs Gold et la publication PostgreSQL, `inspect_silver.py` vérifie les doublons d’`event_id`, les champs obligatoires retenus, la cohérence secondes/minutes et la complétude de `route_id`, `stop_id`, `line_name`, `stop_name`. Une anomalie bloquante interrompt le pipeline ; les événements déjà classés dans `rejected_events` restent consultables. Un enrichissement incomplet n’entraîne pas de déplacement automatique de l’observation vers les rejets : les données restent dans Silver pendant que l’inspection bloque la suite.

Ces contrôles ne garantissent pas la conformité à toutes les exigences du contrat ni la correspondance sémantique de chaque libellé avec son identifiant GTFS. Le contrôle de complétude vérifie des champs renseignés ; il ne détecte pas à lui seul un nom non vide mais incorrect ni une référence périmée.

### Reprise après relancement

`snapshot_resume_state.py` prend désormais `--mode simulation` (valeur par défaut) ou `--mode real`. Le mode sélectionne les tables Silver et le schéma PostgreSQL ; ne pas comparer des captures de modes ou d’installations différents.

Les requêtes Bronze et Silver disposent de checkpoints distincts. PostgreSQL utilise des upserts sur les clés `(window_start, route_id)` et `(event_date, stop_id)`. Les captures comparent le contenu Silver et PostgreSQL, pas les fichiers physiques de checkpoints ni l’historique Delta.

#### Simulation — validation historique conservée

Les captures `data/validation/reprise_before.json` et `reprise_after.json` présentent le même état pour le scénario de simulation documenté. Pour reproduire le contrôle, choisir des noms encore inexistants, sélectionner le bon projet Compose et ne publier aucun événement entre les captures :

```bash
export PYTHONPATH="$PWD/spark/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"

.venv/bin/python scripts/snapshot_resume_state.py \
  data/validation/check_before.json --mode simulation
```

Si la capture réussit, relancer seulement le traitement de simulation :

```bash
(
  unset LAKEHOUSE_ROOT CHECKPOINT_ROOT GTFS_REFERENCE_ROOT
  unset BRONZE_TRIP_UPDATES_PATH BRONZE_TRIP_UPDATES_CHECKPOINT_PATH
  unset POSTGRES_TARGET_SCHEMA
  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export KAFKA_TRIP_UPDATES_TOPIC=sncf.trip_updates.raw
  bash scripts/run_pipeline.sh
)
```

Après réussite du traitement, capturer puis comparer :

```bash
.venv/bin/python scripts/snapshot_resume_state.py \
  data/validation/check_after.json --mode simulation

diff -u data/validation/check_before.json data/validation/check_after.json
```

Arrêter à la première erreur ; une capture après un traitement en échec ne valide pas sa reprise.

#### Réel — comparaison vérifiée le 20 octobre 2026

Dans l’installation originale, une capture a été créée avant une relance de `run_real_pipeline.sh`, puis une seconde après, sans nouvelle publication Kafka :

```text
data/validation/real_resume_2026-10-20_before.json
data/validation/real_resume_2026-10-20_after.json
```

L’état avant comprenait 110 observations Silver, 0 rejet, 7 agrégats ligne/fenêtre et 84 agrégats arrêt/jour dans `sncf_real`. La comparaison a retourné :

```text
Comparaison OK : les captures avant et après sont identiques.
```

Ce résultat valide la stabilité du contenu capturé après une relance normale du parcours à la demande. Il ne démontre ni une récupération après interruption en cours d’écriture, ni une reprise après perte du volume Kafka, ni la reprise du futur mode actif. La procédure reproductible avec un dossier de contrôle neuf figure dans [le runbook](docs/runbook.md).

Le script de capture refuse l’écrasement d’un fichier et limite chaque table Silver capturée à 10 000 lignes ; cette limite appartient au contrôle local. Les captures contenant les données restent locales sauf décision explicite d’archivage après vérification.

Ne pas supprimer les checkpoints pour un redémarrage normal : ils conservent la progression des traitements. La déduplication est bornée par le watermark ; aucune garantie universelle d’exactly-once n’est annoncée. Toute évolution vers le mode actif doit examiner la compatibilité des requêtes, de leur état et des checkpoints avant de les réutiliser.

Pour arrêter les services sans supprimer les volumes :

```bash
docker compose down
```

### Validation depuis un clone isolé

Un test local a été réalisé dans un clone utilisant le projet Compose
`sncf-realtime-lakehouse-cleancheck`, avec l'installation originale arrêtée
pour libérer les ports.

Résultats vérifiés :
- Création du topic `sncf.trip_updates.raw`.
- Deuxième exécution du script de création sans modification du TopicId.
- Initialisation des deux tables PostgreSQL de restitution.
- Exécution de la démonstration jusqu'à la publication Gold dans PostgreSQL.
- Un événement comptabilisé dans chaque agrégat, avec un retard de 2 minutes.
- Relance du pipeline sans nouvel événement : données Silver et PostgreSQL
  identiques avant et après, vérifiées par comparaison des captures JSON.

Ce test a révélé une dépendance au nom fixe du conteneur `sncf-kafka`.
Le script `create_topics.sh` utilise désormais le service Compose `kafka`.

Les captures `cleancheck_before.json` et `cleancheck_after.json` ont été produites dans le clone pendant le test. Leur disponibilité après nettoyage dépend de leur archivage local ; elles ne sont pas automatiquement reconstituées par Git.

Ne pas les confondre avec les anciennes captures `reprise_before.json` et `reprise_after.json`.

Ce contrôle valide le scénario exécuté ; il ne constitue pas une garantie
universelle d'exactly-once ni une validation des dashboards Tableau. La BI a été contrôlée séparément dans l’installation originale.

## 6. Avancement et prochaines étapes

### Implémenté et validé

- [x] Infrastructure Docker Compose : Kafka, Kafka UI et PostgreSQL ; service Metabase historique encore présent.
- [x] Topics de simulation et réel créés par des scripts versionnés.
- [x] Contrat JSON actualisé pour la simulation et le mapping réel, avec limites des contrôles documentées.
- [x] Producteur Java simulé et simulateur Python avec cas invalides.
- [x] Ingestion Bronze, validation / déduplication / enrichissement Silver.
- [x] Rejets Silver traçables et contrôles bloquants avant Gold.
- [x] Agrégats Gold par ligne et par arrêt, publication PostgreSQL par upsert.
- [x] Scripts de démonstration et de traitement de simulation.
- [x] Suites Java/Python confirmées réussies le 20 octobre ; 14 cas ciblés d’enrichissement réussis et suite Python complète confirmée le 27 octobre, sans total global inventé.
- [x] CI GitHub Actions existante pour Java et Python/Spark.
- [x] Dashboard de simulation documenté et capture versionnée.
- [x] Runbook de simulation et validation locale depuis un clone.
- [x] Contrôle de reprise de simulation sans nouvel événement.
- [x] Reprise normale réelle vérifiée le 20 octobre : captures complètes Silver/PostgreSQL identiques avant/après.
- [x] Capture d’état paramétrable : `--mode simulation` / `--mode real`.
- [x] Contrôle bloquant de complétude de l’enrichissement, tests négatifs en mémoire et inspection réelle réussis le 27 octobre.
- [x] Contrat et runbook mis à jour pour le parcours réel, la reprise et le nouveau contrôle d’enrichissement.
- [x] Téléchargement manuel et inspection d'un instantané SNCF réel.
- [x] Lecture Java du GTFS statique, mapping réel et 8 tests dédiés.
- [x] Export JSONL réel complet de l'instantané testé : 16 978 événements.
- [x] Préparation des référentiels réels avec manifeste de provenance.
- [x] Isolation des topics, tables Delta, checkpoints et schémas PostgreSQL.
- [x] Garde-fou du publisher sur le couple lakehouse / schéma.
- [x] Validation historique de 10 événements réels jusqu’à PostgreSQL et Metabase.
- [x] Connexion Tableau Desktop Free Edition → PostgreSQL sur Mac avec pilote PostgreSQL.
- [x] Deux sources Tableau séparées pour les agrégats réels par ligne et par arrêt.
- [x] Dashboard de synthèse : trois KPI, retard moyen par ligne et par arrêt.
- [x] Dashboard d’analyse : quatre indicateurs par arrêt, tri et filtre de période.
- [x] Contrôle SQL des deux lots réels : 110 observations, 7 agrégats ligne/fenêtre et 84 agrégats arrêt/jour.
- [x] Échantillon complémentaire diversifié : 100 observations, 8 courses, 6 lignes et 39 observations > 5 minutes.
- [x] Actualisation manuelle Tableau après nouvelle publication PostgreSQL : passage de 10 à 110 observations.
- [x] Descriptions et formats des dashboards mis à jour ; filtre de période testé sur les dates disponibles.
- [x] Script `run_real_pipeline.sh` exécuté jusqu’à PostgreSQL avec les références du lot, sans nouvel envoi Kafka.
- [x] Captures des deux dashboards disponibles pour présentation dans le README.
- [x] Documentation du bilan réel dans `docs/real-source.md`.
- [x] Téléchargement HTTP Java implémenté dans un composant dédié, avec délais, contrôles et tests présents dans le code.
- [x] Script de collecte/préparation réel présent : références, manifeste, instantané et export JSONL, sans publication Kafka ni Spark.

### Priorité de clôture V1 — mode quasi-temps réel local

Décision du 27 octobre 2026 : ajouter un mode actif avant de déclarer la V1 terminée sur l’objectif « Real-Time ». Les éléments suivants sont une feuille de route, pas des fonctionnalités déjà livrées. Le mode à la demande et les commandes existantes restent disponibles pour les tests et les rejeux.

#### Étape 1 — cadrer les modes et la compatibilité

- [ ] Examiner les classes Java, scripts et jobs Spark actuels avant toute modification.
- [ ] Définir un mode actif explicite sans changer silencieusement le comportement `availableNow` existant.
- [ ] Choisir les paramètres de collecte, de traitement, de volume et d’arrêt après analyse des ressources locales et de la source ; ne pas promettre une latence avant mesure.
- [ ] Examiner la compatibilité des checkpoints et les chemins de validation. Prévoir une validation isolée si nécessaire, sans supprimer les checkpoints ou les données historiques.

#### Étape 2 — collecter et publier automatiquement

- [ ] Ajouter un collecteur/producteur Java actif qui récupère régulièrement les instantanés récents, les normalise et publie les observations retenues dans Kafka.
- [ ] Réutiliser un référentiel GTFS préparé et compatible ; ne pas retélécharger ou reconstruire inutilement les références à chaque interrogation du flux.
- [ ] Rendre la cadence configurable, respecter les conditions d’accès à la source et gérer délais, erreurs HTTP, nouvelles tentatives bornées et arrêt propre.
- [ ] Détecter un instantané inchangé et éviter sa republication aveugle ; conserver les identifiants déterministes et la différence entre doublon et nouvelle observation du même passage.
- [ ] Définir et tester la reprise après publication partielle : ne pas considérer un lot comme entièrement livré avant ses accusés de réception. L’idempotence Kafka seule ne règle pas les renvois entre exécutions.
- [ ] Définir un volume compatible avec le Mac ; si un échantillonnage est nécessaire, le rendre explicite et ne pas annoncer une couverture exhaustive du réseau.

#### Étape 3 — maintenir Bronze et Silver actifs

- [ ] Ajouter un fonctionnement en micro-batches périodiques sans arrêt automatique une fois le stock disponible traité.
- [ ] Conserver validation, traçabilité Kafka, déduplication bornée par watermark, enrichissement et gestion des rejets.
- [ ] Prévoir lancement, arrêt propre, propagation des erreurs et reprise ; les scripts séquentiels actuels attendent la fin des jobs et ne suffisent donc pas à orchestrer des requêtes permanentes.
- [ ] Adapter et tester la stratégie des contrôles de qualité en mode actif, sans présenter les tests batch existants comme une validation suffisante de ce nouveau fonctionnement.

#### Étape 4 — actualiser Gold et PostgreSQL automatiquement

- [ ] Choisir et implémenter une publication par micro-batch ou selon une cadence automatique définie, avec des jobs/sessions gérés explicitement.
- [ ] Préserver la sémantique cumulative des agrégats : ne pas remplacer le total d’une fenêtre ou d’un jour par le résultat de son seul dernier micro-batch.
- [ ] Gérer observations successives, doublons, données tardives et retries sans addition répétée ou perte silencieuse des observations retenues.
- [ ] Respecter l’upsert et les clés existantes ; mesurer le coût des recalculs et éviter de redémarrer toute la chaîne Spark à chaque observation.
- [ ] Vérifier que le volume et la méthode de publication restent compatibles avec la limite actuelle du publisher ; ne pas annoncer sa suppression ou sa scalabilité avant modification et mesure.

#### Étape 5 — mesurer fraîcheur et fiabilité

- [ ] Tracer séparément le timestamp d’observation du flux, l’heure de réception par le collecteur et la disponibilité effective dans PostgreSQL. Le `ingested_at` du payload archivé ne suffit pas à mesurer le délai d’un rejeu.
- [ ] Mesurer la fraîcheur de bout en bout (observation source → PostgreSQL) et le délai propre au pipeline (réception collecteur → PostgreSQL), avec des horloges cohérentes et un périmètre défini.
- [ ] Détecter l’absence de nouvel instantané ou de nouvelles données sans la confondre avec un pipeline à faible latence ; une source stagnante ne doit pas être présentée comme fraîche.
- [ ] Conserver un bilan de démonstration : plusieurs instantanés récents distincts, volumes retenus/rejetés, mesures, anomalies et limites. Aucun objectif chiffré n’est encore déclaré atteint.
- [ ] Tester un arrêt/redémarrage du mode actif et les scénarios d’échec retenus, en distinguant cette preuve du test de relance normale du 20 octobre.

#### Étape 6 — démontrer et documenter

- [ ] Démontrer qu’un seul lancement suffit à collecter, publier et traiter plusieurs instantanés récents, sans commande manuelle intermédiaire, jusqu’à PostgreSQL.
- [ ] Comparer des indicateurs SQL avec les données détaillées ou un jeu de référence sur le même périmètre.
- [ ] Vérifier la consultation dans Tableau après actualisation ; présenter clairement le caractère manuel de l’actualisation visuelle.
- [ ] Mettre à jour les commandes, le runbook, le diagramme et les limites après implémentation, puis enregistrer une démonstration reproductible.

Le critère de réussite est un flux actif jusqu’à PostgreSQL, observé et mesuré pendant une session locale. Il n’exige ni Airflow, ni cloud, ni disponibilité 24 h/24, ni pilotage automatique de Tableau Desktop. Le démarrage manuel initial est acceptable ; l’enchaînement des nouveaux instantanés ne doit plus nécessiter d’intervention manuelle.

### Autres éléments à finaliser pour la V1

- [ ] Vérifier le parcours réel et ses prérequis depuis un clone isolé, séparément de la validation historique de simulation.
- [ ] Compléter les contrôles du contrat encore non couverts si retenus pour la clôture, sans annoncer une conformité totale déjà acquise.
- [ ] Conserver les rapports des suites de tests et vérifier la CI du commit retenu ; une réussite locale ne prouve pas celle d’un nouveau commit sur GitHub.
- [ ] Sécuriser les ports publiés sur l’interface locale et prévoir un compte BI en lecture seule ; ces modifications ne sont pas encore déclarées réalisées.
- [ ] Vérifier le classeur Tableau avant versionnement et tester sa réouverture depuis une configuration reproductible.
- [ ] Archiver les preuves déjà produites : contrôles SQL, reprise réelle et actualisation Tableau, avec des captures propres sans secrets.
- [ ] Actualiser les deux images du README et vérifier le guide Tableau ainsi que leur rendu sur GitHub.
- [ ] Retirer les références et le service Metabase devenus inutiles après vérification des dépendances, sans supprimer les volumes métier.
- [ ] Compléter la documentation d’architecture et les contrôles de qualité de code, sans recréer la CI existante.
- [ ] Ajouter des analyses temporelles seulement après validation de plusieurs fenêtres et d’un historique suffisant ; elles ne sont pas nécessaires pour prouver à elles seules le transport actif du flux.
- [ ] Acter le périmètre de clôture des alertes et de la DLQ Kafka ; ne pas les présenter comme implémentées.

La V1 n’est pas déclarée terminée parce que le rejeu des 110 observations fonctionne : le mode actif choisi comme objectif doit encore être implémenté et démontré.

### Limites actuelles

- Le producteur simulé publie un événement par exécution ; son flag `SIMULATION_MODE=false` n'active pas le parcours réel.
- Le parcours réel reste une suite d’étapes à la demande. La collecte HTTP Java existe, mais le script de préparation n’enchaîne pas la publication Kafka, Spark et PostgreSQL.
- Le périmètre réel validé est limité à 110 observations sélectionnées dans deux instantanés. Les 16 978 événements exportés du premier instantané n’ont pas été validés intégralement de bout en bout ; les 100 nouvelles observations viennent d’un autre lot.
- Les courses ajoutées ou annulées et les arrêts sautés sont exclus du premier périmètre de retard, avec des compteurs d'exclusion.
- Les horaires théoriques réels sont reconstitués depuis `time - delay` ; les fenêtres utilisent la publication du flux.
- Plusieurs instantanés peuvent produire plusieurs observations du même passage : les agrégats actuels ne constituent pas un calcul de ponctualité finale ou de dernière prévision par passage.
- Les traitements actuellement implémentés sont lancés à la demande, pas en continu. La feuille de route V1 du mode actif ne constitue pas une preuve de streaming permanent déjà livré.
- Gold est recalculé avec `overwrite` depuis Silver.
- PostgreSQL est alimenté par upsert ; les lignes qui disparaissent de Gold ne sont pas automatiquement supprimées de PostgreSQL.
- Le publisher collecte les agrégats sur le driver et refuse plus de 10 000 lignes par table.
- La validation Silver ne couvre pas encore toutes les règles du contrat.
- Les UUID déterministes et l'idempotence Kafka ne garantissent pas un exactly-once universel.
- Les dashboards Tableau ont été configurés manuellement. Leur ouverture dépend du classeur et de la base locale ; la reconstruction dans un nouveau clone reste à contrôler.
- La connexion « En direct » n’est pas une collecte ni une actualisation visuelle continue.
- L’échantillon ne permet pas encore une analyse temporelle ou une comparaison représentative des lignes.
- Le taux d’observations > 5 minutes dépend du périmètre : 0 % le 8 octobre, 39 % le 15 octobre et 35,45 % sur les deux lots. Ce n’est pas un taux de trains uniques en retard. Un taux nul au-delà du seuil ne signifie pas absence de tout retard.
- Le compte BI en lecture seule et la restriction du port PostgreSQL à l’interface locale ne sont pas déclarés réalisés.
- Les alertes de service et la DLQ Kafka ne sont pas implémentées.
- Un seul broker Kafka est utilisé : le projet local n'est pas une infrastructure de production haute disponibilité.

### Évolutions envisagées

Airflow reste en V2, après validation du mode quasi-temps réel local : rafraîchissement planifié des références GTFS, contrôles périodiques, recalculs historiques et publications batch selon leurs dépendances. Il ne doit pas consommer chaque événement Kafka, remplacer Spark ni piloter l’interface Tableau Desktop.

Le passage au mode actif est un travail d’ingestion, de traitement et de publication prévu pour la V1 ; l’ajout d’Airflow seul ne le réalise pas. Les futurs DAG n’ont pas encore été implémentés ou validés. La gestion des références en continu et la reproductibilité devront être documentées avant leur orchestration.

Azure reste une trajectoire optionnelle, sans dépendance cloud ni coût imposé à la V1.

Ne pas versionner `.env`, les secrets, les instantanés téléchargés, les exports volumineux, les données Delta ni les checkpoints. Vérifier aussi les informations de connexion et les données éventuellement embarquées dans le classeur Tableau.
