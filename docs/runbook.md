# Runbook — SNCF Real-Time Lakehouse

## 1. Périmètre

Ce runbook décrit l’exécution locale à la demande des parcours de simulation et SNCF réel, ainsi que les contrôles et la reprise sans nouvel événement.

```text
Producteur Java / simulateur Python / export SNCF normalisé
    → Kafka → Bronze Delta → Silver Delta → Gold Delta → PostgreSQL
    → Tableau Desktop sur le Mac pour le parcours réel
```

Metabase reste une restitution historique de simulation ; son service est encore présent dans Docker Compose. Tableau est une application locale, pas un service Docker.

Bronze et Silver utilisent `availableNow=True` : ils traitent les données disponibles puis s’arrêtent. Gold est recalculé en batch, puis publié par upsert dans PostgreSQL. Les contrôles Silver s’exécutent avant Gold et la publication. Ni la collecte ni l’affichage Tableau ne sont actualisés automatiquement en continu.

| Élément | Simulation | Réel |
|---|---|---|
| Topic | `sncf.trip_updates.raw` | `sncf.trip_updates.real.raw` |
| Racine Delta | `data/lakehouse` | `data/lakehouse_real` |
| Checkpoints | `data/checkpoints` | `data/checkpoints_real` |
| Références | `spark/resources/simulation` | `data/reference/real/<lot>` |
| Schéma PostgreSQL | `public` | `sncf_real` |
| Traitement sans publication Kafka | `run_pipeline.sh` | `run_real_pipeline.sh <références>` |
| Capture d’état | `snapshot_resume_state.py --mode simulation` | `snapshot_resume_state.py --mode real` |

Le parcours réel a été vérifié sur 110 observations des instantanés des 8 et 15 octobre 2026. La reprise normale a été vérifiée le 20 octobre. Ce sont des validations locales sur un échantillon, pas une mesure représentative du réseau ni une garantie universelle d’exactly-once.

Les alertes de service et la DLQ Kafka ne sont pas considérées comme implémentées. Les événements rejetés par les contrôles décrits ici sont conservés dans des tables Delta Silver.

Arrêter la procédure à la première erreur. Ne pas republier un échantillon ni supprimer un checkpoint pour contourner une erreur. Les blocs de ce document sont des étapes à exécuter séparément après vérification de leurs prérequis, pas un script à coller intégralement.

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

Renseigner les variables PostgreSQL dans `.env`. Prérequis : Docker Desktop, Docker Compose, JDK 17, Maven et Python 3.12. Pour la BI réelle : Tableau Desktop et son pilote PostgreSQL.

Ne pas conserver globalement les variables d’un autre parcours (`LAKEHOUSE_ROOT`, `CHECKPOINT_ROOT`, `GTFS_REFERENCE_ROOT`, `POSTGRES_TARGET_SCHEMA` et chemins Bronze). Les commandes de simulation ci-dessous isolent leur configuration dans un sous-shell.

Ne jamais écraser un `.env` existant sans sauvegarde et ne jamais
versionner les secrets.

Créer l'environnement Python s'il n'existe pas encore :

```bash
if [ ! -d .venv ]; then
  python3.12 -m venv .venv
fi
source .venv/bin/activate
python -m pip install -r spark/requirements.txt
python -m pip install -r requirements-simulator.txt
```

La version de Python doit être cohérente avec les dépendances du projet
et la configuration des tests.

### Démarrer les services

Pour une première installation :

```bash
docker compose up -d kafka kafka-ui postgres
docker compose ps -a
```

Si les conteneurs existent déjà et avaient simplement été arrêtés :

```bash
docker compose start kafka kafka-ui postgres
docker compose ps -a
```

Consulter les logs :

```bash
docker compose logs --tail=100 kafka postgres
```

Attendre que Kafka et PostgreSQL répondent avant de poursuivre. Le démarrage de Metabase n’est pas requis pour le parcours Tableau. Un `docker compose up -d` sans liste de services démarre aussi Metabase tant qu’il figure dans Compose.

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


### Initialiser le parcours réel — si nécessaire

Les tables `public` précédentes doivent déjà exister : le script SQL réel en reprend la structure. Sur une installation déjà initialisée, contrôler d’abord l’existant ; ne pas recréer un topic ou une base pour résoudre une erreur de reprise.

```bash
bash scripts/create_real_topic.sh

docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -X -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < infra/postgres/002_real_gold_tables.sql
```

Le topic réel validé possède une partition et un facteur de réplication de 1. Vérifier ses caractéristiques et les tables :

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --describe --topic sncf.trip_updates.real.raw

docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -X -v ON_ERROR_STOP=1 \
   -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
   -c "\dt sncf_real.gold_*"'
```

Créer un topic absent ne restaure ni les messages perdus ni sa compatibilité avec un ancien checkpoint. Les données, les références et les checkpoints locaux ne sont pas fournis automatiquement par Git.

## 3. Démonstration complète

### Simulation — produit un nouvel événement

Vérifier qu’aucun autre lancement du pipeline n’est actif. Depuis la racine du dépôt :

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

`run_demo.sh` vérifie le topic, produit un événement Java puis lance Bronze, Silver, les contrôles qualité, Gold et la publication PostgreSQL. Chaque exécution peut ajouter un événement : ne pas l’utiliser pour tester une reprise sans nouvel envoi.

Validation : accusé de réception `status=delivered`, connexion PostgreSQL réussie, contrôles Silver réussis et publication terminée. Les nombres de lignes publiées sont des nombres d’agrégats, pas directement des nombres d’observations.

La restitution Metabase de simulation est historique. Les dashboards Tableau réels utilisent `sncf_real`, pas `public`.

### Réel — retraiter les données déjà publiées

C’est le parcours à choisir pour une relance ou un contrôle des données existantes. Il ne nécessite pas de republier le JSONL.

Préconditions : Kafka et PostgreSQL disponibles, tables réelles initialisées, environnement `.venv` prêt et dossier de références existant. Exemple du lot utilisé lors des validations :

```bash
REFERENCE_DIR="$PWD/data/reference/real/collection-eYyLYD"
```

Ce chemin désigne un dossier local historique ; il n’est pas recréé par Git. Pour un autre lot, utiliser son propre dossier préparé. Vérifier les fichiers avant de lancer le traitement :

```bash
(
  set -e
  test -f "$REFERENCE_DIR/routes.csv"
  test -f "$REFERENCE_DIR/stops.csv"
  test -f "$REFERENCE_DIR/manifest.json"
  bash scripts/run_real_pipeline.sh "$REFERENCE_DIR"
)
```

Le script utilise le topic réel, les racines Delta/checkpoints réelles et le schéma `sncf_real`. Il enchaîne Bronze, Silver, les contrôles Silver, les deux agrégations Gold et la publication PostgreSQL. Il ne collecte aucune source, ne publie aucun message Kafka et n’actualise pas Tableau.

Sur le jeu validé de 110 observations, les publications affichent 7 lignes et 84 lignes. Ces valeurs peuvent changer avec de nouveaux événements ; le message de réussite ne remplace pas les contrôles SQL.

### Réel — préparer et publier un nouveau lot, seulement si nécessaire

Ne pas exécuter cette procédure pour une simple vérification des 110 observations déjà présentes.

1. Disposer d’une archive GTFS statique locale et vérifier sa provenance.
2. Préparer une collecte avec le script dédié ; le chemin ci-dessous doit être remplacé par celui d’une archive existante :

```bash
GTFS_ARCHIVE="/chemin/vers/referentiel_gtfs.zip"
(
  set -e
  test -f "$GTFS_ARCHIVE"
  bash scripts/collect_real_snapshot.sh "$GTFS_ARCHIVE"
)
```

3. Reprendre les chemins de sortie affichés par ce script. Ne pas relancer la préparation dans un dossier déjà créé.
4. Contrôler la source, la date d’observation, les identifiants et le nombre d’événements de l’échantillon. Vérifier qu’il n’a pas déjà été publié. Un lot plus ancien peut nécessiter une procédure historique distincte compte tenu du watermark Silver : ne pas le forcer en supprimant les checkpoints.
5. Renseigner des chemins absolus et un effectif limité, compris entre 1 et 20 000, sans dépasser le nombre de lignes disponibles :

```bash
# Chemins à adapter aux sorties de la nouvelle collecte, pas à recopier tels quels.
JSONL_OUTPUT="/chemin/absolu/vers/trip_updates.canonical.jsonl"
REFERENCE_DIR="/chemin/absolu/vers/les/references_du_lot"
EVENT_COUNT=10
```

6. Après validation de l’échantillon, publier une seule fois :

```bash
(
  set -e
  test -f "$JSONL_OUTPUT"
  test -f "$REFERENCE_DIR/manifest.json"
  cd producer-java
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  mvn --batch-mode --no-transfer-progress compile exec:java \
    -Dexec.mainClass=fr.remyqueny.sncf.producer.RealJsonlProducer \
    -Dexec.args="\"$JSONL_OUTPUT\" $EVENT_COUNT"
)
```

Vérifier `status=preflight_ok`, les accusés de réception et `status=completed ... delivered_count=...`. Le nombre livré doit correspondre à la sélection. Le publisher cible `sncf.trip_updates.real.raw` et utilise `trip_id` comme clé.

En cas d’échec après certains accusés de réception, arrêter et diagnostiquer avant tout nouvel envoi. L’idempotence Kafka ne déduplique pas automatiquement deux exécutions applicatives distinctes. Conserver les identifiants et timestamps du JSONL lors d’un rejeu.

7. Exécuter `bash scripts/run_real_pipeline.sh "$REFERENCE_DIR"`, puis les contrôles SQL et l’actualisation Tableau décrits ci-dessous. La collecte, la publication, le traitement et l’actualisation sont des étapes séparées ; ce n’est pas une orchestration continue.

### Contrôler PostgreSQL — parcours réel

Depuis le terminal habituel, ouvrir `psql` dans le service PostgreSQL :

```bash
docker compose exec postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -X \
   -U "${POSTGRES_USER:-postgres}" \
   -d "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}"'
```

Les requêtes suivantes se collent à l’invite `psql` (`nom_de_base=>` ou `nom_de_base=#`), pas dans le shell habituel :

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

Résultats du jeu vérifié en octobre 2026, en l’absence de nouvelles publications :

| Date | Agrégats arrêt/jour | Observations | Observations > 5 min | Part > 5 min |
|---|---:|---:|---:|---:|
| 2026-10-08 | 10 | 10 | 0 | 0,00 % |
| 2026-10-15 | 74 | 100 | 39 | 39,00 % |

La seconde requête retourne 7 agrégats et 110 observations. Les tables représentent les mêmes observations à des granularités différentes : ne pas additionner leurs volumes. Les 84 agrégats arrêt/jour ne sont pas nécessairement 84 arrêts distincts ; les 7 agrégats ligne/fenêtre ne sont pas nécessairement 7 lignes distinctes.

Quitter `psql` avec `\q` avant de reprendre les commandes du terminal.

### Actualiser Tableau manuellement

1. Ouvrir le classeur local dans Tableau Desktop sur le Mac. Vérifier le pilote et la connexion PostgreSQL : hôte `localhost`, port réellement publié (`5432` dans les validations), base et identifiants configurés localement.
2. Conserver deux sources distinctes : `sncf_real.gold_delay_by_line_15min` et `sncf_real.gold_station_delay_daily`. Ne pas les relier artificiellement ni les joindre directement.
3. En connexion directe, utiliser `Données → [source] → Actualiser` pour chacune. Si une source utilise un extrait, utiliser son actualisation d’extrait.
4. Vérifier le filtre de période de l’analyse des arrêts : 8 octobre seul ou 15 octobre seul, puis une plage contenant les deux. Ce filtre n’est pas annoncé comme global à la synthèse.
5. Comparer les valeurs avec PostgreSQL sur le même périmètre. Sur l’ensemble des deux lots validés, la synthèse affiche 110 observations, 7,68 minutes de moyenne pondérée et 45 minutes de maximum. Les deux valeurs de retard ont été observées dans Tableau ; les contrôles SQL ci-dessus portent sur les volumes et les observations au-delà du seuil.
6. Enregistrer le classeur et préparer des captures propres sans secrets. Vérifier la réouverture et les informations de connexion avant tout versionnement du classeur.

Sur le 15 octobre seul, le lot représente 100 observations dont 39 au-delà de cinq minutes : 39 % des observations, pas des trains uniques. Un filtre excluant le 8 octobre n’efface plus toutes les données si le 15 octobre reste inclus.

Actualiser Tableau ne collecte aucune source et ne lance pas Spark. La connexion « En direct » n’est pas une garantie d’actualisation visuelle automatique. Voir `bi/tableau/README.md` pour les calculs et la reconstruction.

## 4. Reprise sans nouvel événement

### Simulation

Pour traiter les données disponibles et republier Gold :

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
  data/validation/check_before.json --mode simulation
```

Si la capture échoue, arrêter la procédure avant de relancer le pipeline.

Relance :

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

Si le pipeline échoue, diagnostiquer l'erreur avant de poursuivre.

Capture après :

```bash
.venv/bin/python scripts/snapshot_resume_state.py \
  data/validation/check_after.json --mode simulation
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


### Réel — comparaison complète avant/après

Préconditions : aucun producteur actif, aucune nouvelle publication entre les captures, aucun autre job actif et bon projet Compose sélectionné. Une capture ne remplace pas une sauvegarde complète de l’installation.

Le script `snapshot_resume_state.py` utilise le mode `simulation` par défaut. Le mode `real` doit être explicite ; il sélectionne `data/lakehouse_real/silver` et les requêtes qualifiées dans `sncf_real`. La capture est en lecture seule côté PostgreSQL, refuse l’écrasement et limite chaque table Silver à 10 000 lignes. Cette limite n’est pas une limite globale du pipeline.

Exécuter chaque étape séparément dans le même terminal. Arrêter dès qu’une étape échoue ; ne pas créer de capture « après » après un échec du pipeline.

1. Créer un dossier de contrôle neuf et indiquer les références existantes :

```bash
mkdir -p data/validation
VALIDATION_DIR="$(mktemp -d data/validation/real-resume-XXXXXX)"
printf 'Dossier du contrôle : %s\n' "$VALIDATION_DIR"
REFERENCE_DIR="$PWD/data/reference/real/collection-eYyLYD"
```

L’exemple de références correspond au lot historique ; l’adapter si nécessaire. Conserver le chemin affiché pour retrouver les captures.

2. Capturer l’état avant :

```bash
(
  export PYTHONPATH="$PWD/spark/src"
  export PYSPARK_PYTHON="$PWD/.venv/bin/python"
  .venv/bin/python scripts/snapshot_resume_state.py \
    "$VALIDATION_DIR/before.json" --mode real
)
```

3. Si la capture a réussi et si les chemins affichés sont réels, relancer le traitement sans nouvel envoi :

```bash
(
  set -e
  test -f "$REFERENCE_DIR/routes.csv"
  test -f "$REFERENCE_DIR/stops.csv"
  test -f "$REFERENCE_DIR/manifest.json"
  bash scripts/run_real_pipeline.sh "$REFERENCE_DIR"
)
```

4. Si le pipeline a réussi, capturer l’état après :

```bash
(
  export PYTHONPATH="$PWD/spark/src"
  export PYSPARK_PYTHON="$PWD/.venv/bin/python"
  .venv/bin/python scripts/snapshot_resume_state.py \
    "$VALIDATION_DIR/after.json" --mode real
)
```

5. Comparer :

```bash
diff -u "$VALIDATION_DIR/before.json" "$VALIDATION_DIR/after.json" \
  && echo "Comparaison OK : les captures avant et après sont identiques."
```

Une absence de différences correspond à un code 0. Des différences ou une erreur de lecture n’affichent pas le message de réussite : examiner la sortie et ne pas annoncer le contrôle validé.

La comparaison couvre les lignes capturées de `trip_delays`, `rejected_events` et des deux tables PostgreSQL, pas seulement leurs compteurs. Elle ne compare pas les fichiers de checkpoints ni l’historique physique des transactions Delta.

### Résultat constaté le 20 octobre 2026

Le test réel a produit les fichiers locaux :

```text
data/validation/real_resume_2026-10-20_before.json
data/validation/real_resume_2026-10-20_after.json
```

Avant la relance : 110 observations Silver, 0 rejet, 7 agrégats ligne/fenêtre et 84 agrégats arrêt/jour dans `sncf_real`. Le traitement s’est terminé sans nouvelle publication Kafka, et la comparaison a retourné :

```text
Comparaison OK : les captures avant et après sont identiques.
```

Ce résultat valide une relance normale avec les checkpoints existants pour ce scénario. Il ne démontre pas une récupération après interruption en cours d’écriture, une reprise après perte du volume Kafka ni un exactly-once universel.

Conserver les captures localement. Ne pas les ajouter automatiquement à Git : elles contiennent les données de l’échantillon. Versionner plutôt la procédure, le script et un bilan sans secrets ; vérifier explicitement tout artefact de preuve destiné au dépôt.

## 5. Contrôles Silver

Les pipelines exécutent l’inspection avant les agrégations Gold. Pour une inspection indépendante, sélectionner explicitement le périmètre ; ces commandes ne doivent pas être exécutées pendant un autre traitement.

### Simulation

```bash
(
  export PYTHONPATH="$PWD/spark/src"
  export PYSPARK_PYTHON="$PWD/.venv/bin/python"
  export LAKEHOUSE_ROOT="$PWD/data/lakehouse"
  .venv/bin/python spark/src/jobs/inspect_silver.py
)
```

### Réel

```bash
(
  export PYTHONPATH="$PWD/spark/src"
  export PYSPARK_PYTHON="$PWD/.venv/bin/python"
  export LAKEHOUSE_ROOT="$PWD/data/lakehouse_real"
  .venv/bin/python spark/src/jobs/inspect_silver.py
)
```

Vérifier dans les résultats le périmètre lu, les effectifs valides/rejetés et les contrôles bloquants :

- Absence de doublons d’`event_id` dans la table des événements valides.
- Champs effectivement contrôlés : `event_id`, `trip_id`, `event_timestamp`, `delay_seconds`.
- Cohérence entre `delay_seconds` et `delay_minutes`.
- Complétude de l’enrichissement via `check_enrichment` : `route_id`, `stop_id`, `line_name` et `stop_name` non nuls et non vides après suppression des espaces en début et fin (`trim`).

Les quatre messages de réussite attendus sont :

```text
Contrôle des doublons : OK
Contrôle des champs obligatoires : OK
Contrôle de cohérence des retards : OK
Contrôle de l'enrichissement : OK
```

En cas d’enrichissement incomplet, l’inspection affiche jusqu’à dix lignes concernées puis lève une `RuntimeError`. Le pipeline doit s’arrêter avant Gold et la publication PostgreSQL. Les observations restent dans Silver : elles ne sont pas automatiquement déplacées dans `rejected_events` et aucune donnée n’est réécrite par l’inspection.

Ce contrôle vérifie que les quatre champs sont renseignés, pas que chaque nom est sémantiquement correct pour l’identifiant GTFS ni que le référentiel est à jour. Toutes les exigences du contrat ne sont pas encore contrôlées. Voir `docs/data-contract.md` pour la couverture et les limites, notamment les timestamps du payload et les retards négatifs autorisés.

Tables de rejets :

```text
Simulation : data/lakehouse/silver/rejected_events
Réel       : data/lakehouse_real/silver/rejected_events
```

Ne pas confondre un événement classé comme rejet, une exclusion avant Kafka, un doublon éliminé et une erreur technique. Zéro rejet ne démontre pas à lui seul l’exhaustivité du flux.

Une anomalie bloquante de l’inspection doit arrêter la procédure avant Gold et PostgreSQL. Lire la première erreur ; ne pas supprimer les données ou les checkpoints pour obtenir un contrôle vert.

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

### Tests du contrôle d’enrichissement

Pour exécuter uniquement les nouveaux cas, sans lecture ni écriture des tables métier :

```bash
PYTHONPATH="$PWD/spark/src" \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest \
  spark/tests/test_trip_updates.py \
  -k "enrichment_check" -v
```

Les 14 cas couvrent une ligne complète, douze valeurs manquantes (quatre champs × `null`, chaîne vide, espaces) et un mélange de lignes complètes et incomplètes. Ils utilisent des DataFrames créés en mémoire ; aucun producteur Kafka ni job de publication PostgreSQL n’est lancé.

Validation du 27 octobre 2026 :

- Résultat ciblé observé : `14 passed, 38 deselected in 8.95s`. Les 38 cas désélectionnés étaient exclus par le filtre, pas en échec.
- Suite Python complète ensuite confirmée sans erreur ; aucun nouveau total précis n’est déduit de ce retour.
- Inspection Silver réelle exécutée ensuite et confirmée sans erreur, y compris le nouveau contrôle. Les compteurs de cette inspection n’ont pas été recopiés dans le bilan : ne pas en déduire un nouvel effectif.

Après une modification de ce contrôle, exécuter d’abord les tests ciblés, puis la suite Python complète et enfin l’inspection indépendante du périmètre voulu (section 5). Ne pas republier des événements pour tester un cas invalide : les cas négatifs restent en mémoire.

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
les tests unitaires et de transformation ne remplacent pas un test avec Kafka/PostgreSQL et une vérification séparée de Tableau. La CI ne lance pas l’application Tableau.

Ne pas annoncer un nouveau nombre de tests passés sur la seule base d’une modification documentaire ou du nombre de fonctions présentes dans le code.

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

Cette commande concerne la simulation : `sncf.trip_updates.raw`.

Pour le réel, après vérification de l’installation et du topic attendu :

```bash
bash scripts/create_real_topic.sh
```

Le topic réel est `sncf.trip_updates.real.raw`. Ne pas recréer un topic perdu puis réutiliser aveuglément un ancien checkpoint ; diagnostiquer d’abord la cohérence entre messages, offsets et volumes.

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

`run_pipeline.sh` et `run_real_pipeline.sh` configurent ces variables pour leurs traitements. Une commande Python lancée séparément nécessite sa propre configuration.

### Contrôle Silver en échec

Lire les résultats de `inspect_silver.py`, puis consulter :

```text
Simulation : data/lakehouse/silver/trip_delays
Simulation : data/lakehouse/silver/rejected_events
Réel       : data/lakehouse_real/silver/trip_delays
Réel       : data/lakehouse_real/silver/rejected_events
```

Ne pas supprimer les tables Delta ni les checkpoints pour corriger
un problème d'enrichissement ou de qualité.

### Enrichissement non résolu

Le contrôle `check_enrichment` échoue si un des champs `route_id`, `stop_id`, `line_name` ou `stop_name` est nul, vide ou composé uniquement d’espaces. La `RuntimeError` contient `enrichissement incomplet` et les lignes affichées donnent les identifiants à examiner.

1. Identifier les champs manquants dans les lignes affichées.
2. Vérifier le périmètre réel/simulation et le dossier de références utilisé lors de l’enrichissement.
3. Rechercher les identifiants concernés dans les références du lot et contrôler les noms associés.
4. Déterminer une procédure de correction des données existantes avant de relancer le pipeline ; modifier une référence ne met pas automatiquement Silver à jour.

Ne pas remplacer un nom absent par une valeur artificielle pour rendre le contrôle vert. Ne pas déplacer manuellement l’événement vers les rejets ni supprimer son checkpoint pour masquer l’anomalie. Ce contrôle de complétude ne suffit pas à détecter un nom renseigné mais incorrect.

En simulation, les noms proviennent de :

```text
spark/resources/simulation/routes.csv
spark/resources/simulation/stops.csv
```

Dans le parcours réel, utiliser `data/reference/real/<lot>/routes.csv`, `stops.csv` et `manifest.json`. Vérifier que le dossier passé au script correspond au lot source. Ne pas remplacer ces références par les CSV simulés.

Modifier un CSV ne réécrit pas automatiquement les événements déjà
présents dans Silver.

Une correction des données existantes doit faire l'objet d'une
procédure explicitement documentée.

### SIMULATION_MODE=false

Le producteur Java échoue volontairement avec le message :

```text
SIMULATION_MODE=false is not implemented yet.
```

Ce flag du producteur initial n’active pas le parcours réel. Celui-ci utilise des classes dédiées pour télécharger, normaliser et publier : `GtfsRealtimeHttpDownloader`, `GtfsRealtimeJsonExporter` et `RealJsonlProducer`, ainsi que les scripts de collecte et de traitement décrits en section 3.

### Dashboard Metabase historique vide ou en erreur

- Vérifier que PostgreSQL et Metabase sont démarrés.
- Vérifier que les deux tables Gold contiennent des données.
- Vérifier les filtres du dashboard.
- Vérifier la connexion Metabase : hôte `postgres`, port `5432`.
- Actualiser le dashboard après une publication réussie.

Ne pas recréer automatiquement le compte ou le dashboard :
commencer par vérifier la connexion et les données existantes.

### Référence réelle introuvable

Vérifier le chemin fourni, puis les trois fichiers `routes.csv`, `stops.csv` et `manifest.json`. Un dossier de collecte comme `collection-eYyLYD` est un résultat local, pas un nom garanti dans chaque installation. Ne pas créer des fichiers vides pour contourner le précontrôle ; retrouver ou préparer les références du lot.

### Erreur de reprise Kafka ou de checkpoint

Conserver l’erreur complète et vérifier le bon projet Compose, le topic réel/simulé, les volumes et le répertoire de checkpoints. Un conteneur récemment recréé ne permet pas, à lui seul, de conclure que ses données persistantes ont été conservées ou perdues.

Ne pas supprimer le checkpoint, ne pas désactiver la détection de perte de données et ne pas republier tout un export pour masquer une incohérence. Une réinitialisation ou un rejeu historique doit avoir sa propre procédure et sa sauvegarde ; ce runbook ne présente pas de commande de réinitialisation automatique.

### Table Delta occupée ou traitements concurrents

Vérifier qu’aucun autre pipeline ou inspection n’est actif sur les mêmes chemins. Conserver les logs et identifier le processus responsable avant toute intervention. Ne pas effacer `_delta_log`, des fichiers Delta ou des checkpoints pour libérer une table. Lancer un seul scénario de validation à la fois.

### Capture déjà existante ou comparaison différente

Le refus d’écraser une capture est un garde-fou. Choisir un nouveau dossier de contrôle ; conserver l’ancienne preuve. Si le test a été interrompu, ne pas mélanger les captures de deux essais.

Si `diff` affiche des différences, contrôler d’abord : nouvelle publication, autre job actif, autre mode, autre projet Compose, correction de références ou données réellement modifiées. Ne pas écraser les captures pour obtenir artificiellement un résultat identique.

### Tableau ne se connecte pas ou affiche d’anciennes données

1. Vérifier PostgreSQL, le port publié et la réussite d’une requête SQL.
2. Vérifier le pilote Tableau et utiliser `localhost` depuis le Mac, pas le nom Docker `postgres`.
3. Vérifier base, schéma `sncf_real`, identifiants et droits de lecture, sans partager les secrets.
4. Vérifier la réussite du traitement et de la publication Gold : actualiser Tableau ne les déclenche pas.
5. Actualiser les deux sources ; contrôler si la connexion est directe ou repose sur un extrait.
6. Contrôler le filtre de période, la granularité et les calculs pondérés. Une plage « aujourd’hui » ne convient pas aux instantanés historiques des 8 et 15 octobre.
7. Si les chiffres diffèrent, comparer SQL et Tableau sur exactement le même périmètre. Ne pas modifier les formules avant d’avoir identifié la cause.

Pour une capture finale, rendre toutes les colonnes visibles, notamment le retard maximal, et utiliser les descriptions correspondant aux données réellement présentes.

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
