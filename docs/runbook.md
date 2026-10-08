# Runbook — SNCF Real-Time Lakehouse

## 1. Périmètre

Ce runbook décrit l'exécution locale du pipeline en mode simulation.

Le flux actuel est :

```text
Producteur Java
    → Kafka
    → Bronze Delta
    → Silver Delta
    → contrôles qualité Silver
    → Gold Delta
    → PostgreSQL
    → Metabase
```

Bronze et Silver utilisent `availableNow=True` : ils traitent les données
disponibles puis s'arrêtent. Gold est recalculé en batch.

Les événements, lignes et arrêts utilisés actuellement sont simulés.
L'API SNCF réelle, les alertes de service et la DLQ Kafka ne sont pas
considérées comme validées dans ce runbook.

## 2. Préparation

Depuis la racine du dépôt :

```bash
cp .env.example .env
```

Renseigner les variables PostgreSQL dans `.env`, puis créer l'environnement :

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r spark/requirements.txt
python -m pip install -r requirements-simulator.txt
```

Démarrer les services :

```bash
docker compose up -d
docker compose ps
```

Vérifier Kafka et PostgreSQL :

```bash
docker compose logs --tail=100 kafka postgres
```

Créer le topic si nécessaire :

```bash
bash scripts/create_topics.sh
```

Initialiser les tables PostgreSQL si nécessaire :

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < infra/postgres/001_gold_tables.sql
```

## 3. Démonstration complète

La commande suivante produit un nouvel événement :

```bash
bash scripts/run_demo.sh
```

Elle :

1. Vérifie le topic Kafka.
2. Lance le producteur Java une fois.
3. Exécute Bronze.
4. Exécute Silver.
5. Exécute les contrôles qualité Silver.
6. Recalcule Gold.
7. Publie Gold dans PostgreSQL.

Chaque lancement de `run_demo.sh` ajoute potentiellement un nouvel événement.
Ne pas utiliser cette commande pour tester une reprise sans nouvel envoi.

## 4. Reprise sans nouvel événement

Pour retraiter uniquement les données disponibles :

```bash
bash scripts/run_pipeline.sh
```

Cette commande ne lance pas le producteur Java.

Elle utilise les checkpoints existants. Ne pas supprimer les checkpoints
pour un redémarrage normal et ne pas utiliser `docker compose down -v`.

Pour comparer l'état avant et après une reprise :

```bash
export PYTHONPATH="$PWD/spark/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"

.venv/bin/python scripts/snapshot_resume_state.py \
  data/validation/check_before.json

bash scripts/run_pipeline.sh

.venv/bin/python scripts/snapshot_resume_state.py \
  data/validation/check_after.json

diff -u \
  data/validation/check_before.json \
  data/validation/check_after.json
```

Une sortie vide de `diff` signifie que les contenus capturés sont identiques
pour ce scénario.

## 5. Contrôles Silver

Le contrôle est exécuté avant Gold et PostgreSQL :

```bash
export PYTHONPATH="$PWD/spark/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"

.venv/bin/python spark/src/jobs/inspect_silver.py
```

Il vérifie :

- Les doublons d'`event_id`.
- Les champs obligatoires retenus.
- La cohérence entre `delay_seconds` et `delay_minutes`.
- Le nombre d'événements valides et rejetés.
- Les raisons de rejet disponibles dans `rejected_events`.

Une anomalie bloquante arrête le pipeline grâce à `set -euo pipefail`.

## 6. Tests

Tests Java :

```bash
(cd producer-java && mvn test)
```

Tests Python :

```bash
PYTHONPATH="$PWD/spark/src" \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest spark/tests -v
```

La CI GitHub Actions est définie dans :

```text
.github/workflows/ci.yml
```

Elle exécute les tests Java et Python/Spark, mais ne démarre pas Kafka,
PostgreSQL ou Metabase.

## 7. Dépannage

### Kafka indisponible

```bash
docker compose ps kafka
docker compose logs --tail=100 kafka
docker compose start kafka
```

Puis vérifier le topic :

```bash
bash scripts/create_topics.sh
```

Ne pas supprimer le volume Kafka lors d'un redémarrage normal.

### PostgreSQL indisponible

```bash
docker compose ps postgres
docker compose logs --tail=100 postgres
docker compose start postgres
```

Vérifier la connexion :

```bash
docker compose exec -T postgres pg_isready
```

### Topic absent

```bash
bash scripts/create_topics.sh
```

Le topic actuel est :

```text
sncf.trip_updates.raw
```

### Contrôle Silver en échec

Lire les lignes affichées par `inspect_silver.py`, puis consulter :

```text
data/lakehouse/silver/trip_delays
data/lakehouse/silver/rejected_events
```

Ne pas supprimer les tables Delta ni les checkpoints pour corriger un
problème d'enrichissement ou de qualité.

### Enrichissement non résolu

Les noms proviennent de :

```text
spark/resources/simulation/routes.csv
spark/resources/simulation/stops.csv
```

Modifier un CSV ne réécrit pas automatiquement les événements déjà présents
dans Silver. Une correction d'un événement existant doit faire l'objet d'une
procédure explicitement documentée.

### SIMULATION_MODE=false

Le producteur Java échoue volontairement avec le message :

```text
SIMULATION_MODE=false is not implemented yet.
```

L'API réelle n'est pas implémentée dans la version actuelle.

## 8. Arrêt des services

Pour arrêter les services sans supprimer les volumes :

```bash
docker compose down
```

Ne pas utiliser :

```bash
docker compose down -v
```

sauf si une réinitialisation destructive est explicitement souhaitée.