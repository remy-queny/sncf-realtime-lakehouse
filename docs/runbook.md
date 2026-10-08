# Runbook — SNCF Real-Time Lakehouse

## 1. Périmètre

Ce runbook décrit l'exécution locale du pipeline en mode simulation.

Le flux de données actuel est :

```text
Producteur Java / simulateur Python
    → Kafka
    → Bronze Delta
    → Silver Delta
    → Gold Delta
    → PostgreSQL
    → Metabase
```

Des contrôles qualité Silver sont également exécutés par le pipeline.

Bronze et Silver utilisent `availableNow=True` : ils traitent les données
disponibles puis s'arrêtent. Gold est recalculé en batch.

Les événements, lignes et arrêts utilisés actuellement sont simulés.
L'API SNCF réelle, les alertes de service et la DLQ Kafka ne sont pas
considérées comme validées dans ce runbook.

Actualiser Metabase ne déclenche pas les traitements Spark.

## 2. Préparation

### Sélectionner le bon projet

Exécuter les commandes depuis la racine du dépôt original :

```bash
export COMPOSE_PROJECT_NAME=sncf-realtime-lakehouse
```

Cette variable est transmise aux commandes Compose exécutées par les
scripts Bash et par `snapshot_resume_state.py`.

Pour le clone de validation, utiliser depuis sa propre racine :

```bash
export COMPOSE_PROJECT_NAME=sncf-realtime-lakehouse-cleancheck
```

Ne pas démarrer simultanément les deux installations avec les ports
actuels : elles utilisent les mêmes ports sur la machine.

Après un changement de dépôt, vérifier le répertoire courant et le projet :

```bash
pwd
printf 'Projet Compose : %s\n' "$COMPOSE_PROJECT_NAME"
docker compose ps -a
```

### Configurer l'environnement

Créer `.env` uniquement s'il n'existe pas déjà :

```bash
if [ ! -f .env ]; then
  cp .env.example .env
fi
```

Renseigner les variables PostgreSQL dans `.env`.

Ne jamais écraser un `.env` existant sans sauvegarde et ne jamais
versionner les secrets.

Créer l'environnement Python s'il n'existe pas encore :

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r spark/requirements.txt
python -m pip install -r requirements-simulator.txt
```

La version de Python doit être cohérente avec les dépendances du projet
et la configuration des tests.

### Démarrer les services

Pour une première installation :

```bash
docker compose up -d
docker compose ps -a
```

Si les conteneurs existent déjà et avaient simplement été arrêtés :

```bash
docker compose start
docker compose ps -a
```

Consulter les logs :

```bash
docker compose logs --tail=100 kafka postgres
```

Attendre que Kafka et PostgreSQL répondent avant de poursuivre.

### Initialiser Kafka

```bash
bash scripts/create_topics.sh
```

Le script utilise le service Compose `kafka`, pas le nom fixe
du conteneur `sncf-kafka`.

Validation :

- Le topic `sncf.trip_updates.raw` existe.
- Il possède une partition.
- Son facteur de réplication vaut 1.
- Une deuxième exécution conserve le même `TopicId`.

L'avertissement concernant les points et les underscores dans les noms
de métriques n'est pas, à lui seul, une erreur de création.

### Initialiser PostgreSQL

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < infra/postgres/001_gold_tables.sql
```

Vérifier les tables :

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
   -c "\dt public.gold_*"'
```

Tables attendues :

- `gold_delay_by_line_15min`
- `gold_station_delay_daily`

## 3. Démonstration complète

Vérifier qu'aucun autre lancement du pipeline n'est encore actif.

La commande suivante produit un nouvel événement :

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TRIP_UPDATES_TOPIC=sncf.trip_updates.raw \
bash scripts/run_demo.sh
```

Elle vérifie le topic, lance le producteur Java, puis appelle
`run_pipeline.sh` pour exécuter les traitements et la publication.

Les opérations comprennent :

- Ingestion Bronze.
- Traitement Silver.
- Agrégations Gold.
- Publication dans PostgreSQL.
- Contrôles qualité Silver.

Chaque lancement réussi de `run_demo.sh` produit un nouvel événement
simulé. Ne pas utiliser cette commande pour tester une reprise sans
nouvel envoi.

Validation :

- Le producteur affiche `status=delivered`.
- La connexion PostgreSQL réussit.
- La publication Gold se termine sans erreur.
- Le pipeline et la démonstration se terminent.
- Les tables PostgreSQL contiennent les agrégats attendus.

Les nombres de lignes publiées sont des nombres d'agrégats,
pas directement des nombres d'événements.

## 4. Reprise sans nouvel événement

Pour traiter les données disponibles et republier Gold :

```bash
bash scripts/run_pipeline.sh
```

Cette commande ne lance pas le producteur Java.

Elle utilise les checkpoints existants. Ne pas supprimer les checkpoints
pour un redémarrage normal.

### Comparer l'état avant et après

Exécuter toutes les étapes dans le même terminal, avec le projet Compose
correctement sélectionné.

Ne pas produire d'événement entre les deux captures et ne pas lancer
plusieurs pipelines simultanément.

```bash
export PYTHONPATH="$PWD/spark/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
```

Choisir deux noms de fichiers encore inexistants.

Capture avant :

```bash
.venv/bin/python scripts/snapshot_resume_state.py \
  data/validation/check_before.json
```

Si la capture échoue, arrêter la procédure avant de relancer le pipeline.

Relance :

```bash
bash scripts/run_pipeline.sh
```

Si le pipeline échoue, diagnostiquer l'erreur avant de poursuivre.

Capture après :

```bash
.venv/bin/python scripts/snapshot_resume_state.py \
  data/validation/check_after.json
```

Comparaison :

```bash
diff -u \
  data/validation/check_before.json \
  data/validation/check_after.json
```

Validation : aucune sortie de `diff` et code de retour 0.

Cela confirme que les contenus Silver et PostgreSQL capturés sont
identiques pour ce scénario. Ce n'est pas une garantie universelle
d'exactly-once.

Le script refuse d'écraser un fichier existant et limite le contrôle
à 10 000 lignes par table Silver.

Ne pas comparer une ancienne capture d'une installation avec une
nouvelle capture d'une autre installation.

## 5. Contrôles Silver

Le contrôle peut également être exécuté séparément :

```bash
export PYTHONPATH="$PWD/spark/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"

.venv/bin/python spark/src/jobs/inspect_silver.py
```

Consulter les contrôles et résultats affichés par la version actuelle
de `inspect_silver.py`, notamment :

- Les doublons d'`event_id`.
- Les champs obligatoires contrôlés.
- La cohérence entre `delay_seconds` et `delay_minutes`.

Les événements rejetés sont conservés dans :

```text
data/lakehouse/silver/rejected_events
```

Ne pas confondre un événement rejeté selon les règles prévues avec
une anomalie technique du pipeline.

Une commande qui retourne un code d'erreur non nul arrête
`run_pipeline.sh`, qui utilise `set -euo pipefail`.

## 6. Tests

### Tests Java

```bash
(cd producer-java && mvn test)
```

### Tests Python

```bash
PYTHONPATH="$PWD/spark/src" \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest spark/tests -v
```

### CI GitHub Actions

Vérifier la présence du workflow :

```bash
ls .github/workflows/ci.yml
```

Si le workflow existe, consulter sa configuration pour connaître
les versions de Java et Python ainsi que les étapes exécutées.

La présence du fichier ne prouve pas la réussite de la CI.
Vérifier le résultat de l'exécution dans l'onglet Actions de GitHub.

Distinguer les tests Java/Python des tests d'intégration :
un workflow qui ne démarre pas Kafka, PostgreSQL et Metabase
ne valide pas la démonstration complète.

## 7. Dépannage

### Conteneur arrêté ou mauvais projet ciblé

```bash
pwd
printf 'Projet Compose : %s\n' "$COMPOSE_PROJECT_NAME"
docker compose ps -a
```

Vérifier que le terminal se trouve dans le bon dépôt et que le projet
Compose correspond à l'installation visée.

Si une erreur mentionne encore `sncf-kafka`, vérifier que le script
corrigé est bien présent dans le dépôt utilisé :

```bash
grep -n 'docker exec\|docker compose exec' scripts/*.sh
```

Ne pas redémarrer l'autre installation uniquement pour contourner
une erreur de ciblage.

### Kafka indisponible

```bash
docker compose ps -a kafka
docker compose logs --tail=100 kafka
```

Si Kafka avait simplement été arrêté volontairement :

```bash
docker compose start kafka
```

S'il s'arrête de nouveau, corriger la première erreur des logs avant
de multiplier les relances.

Vérifier ensuite les topics :

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list
```

Ne pas supprimer le volume Kafka lors d'un redémarrage normal.

### Topic absent

Après avoir vérifié que Kafka répond :

```bash
bash scripts/create_topics.sh
```

Le topic actuel est :

```text
sncf.trip_updates.raw
```

### PostgreSQL indisponible

```bash
docker compose ps -a postgres
docker compose logs --tail=100 postgres
```

Si PostgreSQL avait simplement été arrêté volontairement :

```bash
docker compose start postgres
```

Vérifier la disponibilité du serveur :

```bash
docker compose exec -T postgres sh -c \
  'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Puis tester réellement la connexion et une requête :

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1;"'
```

Validation : la requête retourne `1`.

Ce test concerne la connexion interne au conteneur.
Le pipeline Python utilise actuellement `localhost:5432` depuis
la machine hôte.

Si le test interne réussit mais que le pipeline échoue, vérifier
le port publié et les paramètres de connexion du pipeline.

Ne jamais transmettre le mot de passe ni le contenu complet de `.env`.

### Module Python introuvable

Pour une erreur comme `No module named 'transforms'` ou
`No module named 'jobs'` :

```bash
export PYTHONPATH="$PWD/spark/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
```

Utiliser `.venv/bin/python` depuis la racine du dépôt.

`run_pipeline.sh` configure lui-même ces variables.

### Contrôle Silver en échec

Lire les résultats de `inspect_silver.py`, puis consulter :

```text
data/lakehouse/silver/trip_delays
data/lakehouse/silver/rejected_events
```

Ne pas supprimer les tables Delta ni les checkpoints pour corriger
un problème d'enrichissement ou de qualité.

### Enrichissement non résolu

Les noms proviennent de :

```text
spark/resources/simulation/routes.csv
spark/resources/simulation/stops.csv
```

Modifier un CSV ne réécrit pas automatiquement les événements déjà
présents dans Silver.

Une correction des données existantes doit faire l'objet d'une
procédure explicitement documentée.

### SIMULATION_MODE=false

Le producteur Java échoue volontairement avec le message :

```text
SIMULATION_MODE=false is not implemented yet.
```

La connexion à l'API réelle n'est pas implémentée dans cette version.

### Dashboard Metabase vide ou en erreur

- Vérifier que PostgreSQL et Metabase sont démarrés.
- Vérifier que les deux tables Gold contiennent des données.
- Vérifier les filtres du dashboard.
- Vérifier la connexion Metabase : hôte `postgres`, port `5432`.
- Actualiser le dashboard après une publication réussie.

Ne pas recréer automatiquement le compte ou le dashboard :
commencer par vérifier la connexion et les données existantes.

## 8. Arrêt des services

Pour arrêter les services en conservant les conteneurs :

```bash
docker compose stop
```

Pour retirer les conteneurs et les réseaux du projet tout en conservant
les volumes nommés, utiliser :

```bash
docker compose down
```

Ne pas utiliser :

```bash
docker compose down -v
```

sauf si une réinitialisation destructive a été explicitement décidée,
avec sauvegarde et compréhension des données supprimées.

Ne pas supprimer les checkpoints ou les fichiers Delta pendant
un traitement actif.

## 9. Basculer entre le clone et l'original

### Arrêter le clone

Depuis la racine du clone :

```bash
docker compose -p sncf-realtime-lakehouse-cleancheck stop
docker compose -p sncf-realtime-lakehouse-cleancheck ps -a
```

Validation : les services du clone sont arrêtés.

### Redémarrer l'original

Depuis la racine du dépôt original :

```bash
export COMPOSE_PROJECT_NAME=sncf-realtime-lakehouse

docker compose -p sncf-realtime-lakehouse start
docker compose -p sncf-realtime-lakehouse ps -a
```

Validation : les services de l'original sont démarrés.

Ne pas relancer `run_demo.sh` uniquement pour vérifier cette bascule :
cela produirait un nouvel événement.

### Reporter une correction

Copier uniquement les fichiers corrigés nécessaires au fonctionnement.

Ne pas recopier automatiquement :

- Le nom de projet Compose spécifique au clone.
- Le fichier `.env`.
- Les données Delta.
- Les checkpoints.
- Les captures d'un ancien test pour remplacer celles d'un nouveau test.

Conserver les captures du clone comme preuves distinctes de validation.