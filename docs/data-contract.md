# Contrat de données — Trip Update

## Objectif

Ce contrat décrit le payload JSON canonique publié dans Kafka pour les observations de retard ferroviaire du projet, en simulation et dans le parcours SNCF réel.

Il est utilisé par :

- Le producteur Java de simulation : `ProducerApplication`.
- Le simulateur Python complémentaire.
- Le parcours réel : normalisation GTFS-RT par `GtfsRealtimeJsonExporter`, puis publication d’un échantillon JSONL par `RealJsonlProducer`.

Le téléchargement HTTP, la normalisation, la publication Kafka et les traitements Spark sont des étapes distinctes. Le flag `SIMULATION_MODE=false` du producteur initial ne remplace pas les classes dédiées au parcours réel.

Bronze conserve le payload JSON original et les métadonnées Kafka. Silver produit des colonnes typées et enrichies. Gold contient des agrégats : il ne reprend pas le format événementiel de ce contrat.

Ce document distingue les règles attendues d’un événement conforme, les choix du mapping réel et les contrôles effectivement présents dans Silver. Une règle décrite ici ne constitue pas, à elle seule, un contrôle implémenté.

## Topic Kafka

| Parcours | Topic | Source du payload | Racine Delta | Schéma PostgreSQL |
|---|---|---|---|---|
| Simulation Java | `sncf.trip_updates.raw` | `simulation_java` | `data/lakehouse` | `public` |
| Simulation Python | `sncf.trip_updates.raw` | `simulation` | `data/lakehouse` | `public` |
| Réel SNCF | `sncf.trip_updates.real.raw` | `sncf_gtfs_rt` | `data/lakehouse_real` | `sncf_real` |

Les topics, les racines Delta et les checkpoints des deux parcours doivent rester séparés. Le topic réel ne doit pas recevoir les événements simulés.

Le périmètre de ce contrat est `trip_update`. Les alertes de service ne sont pas couvertes par ce schéma. La DLQ Kafka n’est pas présentée comme implémentée : les rejets actuellement décrits plus bas sont des tables Delta Silver.

## Clé Kafka

La clé Kafka d’un événement conforme est :

```text
trip_id
```

Elle sert au regroupement des observations d’une même course dans le partitionnement Kafka. Elle ne doit pas être confondue avec `event_id`, qui identifie une observation et sert à la déduplication.

L’ordre des messages dans une partition ne doit pas être interprété comme un tri des heures de passage des trains ni comme un ordre global entre toutes les partitions. Les événements volontairement invalides du simulateur relèvent de tests négatifs, pas du contrat nominal.

## Exemple d’événement

### Simulation Java

```json
{
  "event_id": "a4f8f3b2-1fe8-4f73-bb19-40faef95b98d",
  "event_type": "trip_update",
  "source": "simulation_java",
  "schema_version": 1,
  "trip_id": "TRAIN-H-005",
  "route_id": "H",
  "stop_id": "stop_87271010",
  "scheduled_timestamp": "2026-10-08T14:30:00Z",
  "estimated_timestamp": "2026-10-08T14:30:00Z",
  "delay_seconds": 0,
  "event_timestamp": "2026-10-08T14:25:00Z",
  "ingested_at": "2026-10-08T14:25:02Z"
}
```

Cet exemple est fictif et correspond aux identifiants du modèle H Java. Le producteur simulé génère un nouvel UUID et de nouveaux timestamps à chaque exécution. Les noms de lignes et d’arrêts sont ajoutés dans Silver à partir des références de simulation.

### Illustration du format réel avec une avance

```json
{
  "event_id": "b7ad81c5-3974-3f8d-9873-5dcf2fc173ef",
  "event_type": "trip_update",
  "source": "sncf_gtfs_rt",
  "schema_version": 1,
  "trip_id": "TRIP-EXEMPLE",
  "route_id": "ROUTE-EXEMPLE",
  "stop_id": "STOP-EXEMPLE",
  "scheduled_timestamp": "2026-10-15T14:30:00Z",
  "estimated_timestamp": "2026-10-15T14:28:00Z",
  "delay_seconds": -120,
  "event_timestamp": "2026-10-15T14:25:00Z",
  "ingested_at": "2026-10-15T14:25:02Z"
}
```

Cet exemple est entièrement illustratif : les identifiants et l’UUID ne proviennent pas d’une observation SNCF et ne sont pas le résultat du calcul du mapper. Il montre uniquement qu’une valeur négative représente une avance et reste cohérente avec les deux horaires. Il ne doit pas être publié dans le topic réel comme une donnée source.

## Description des champs

Dans ce tableau, « obligatoire » décrit le contrat attendu, pas la totalité des vérifications Silver.

| Champ | Type du payload | Obligatoire | Description |
|---|---|---:|---|
| `event_id` | string UUID | Oui | Identifiant de l’observation. UUID nouveau en simulation ; déterministe dans le mapping réel. Clé de déduplication Silver. |
| `event_type` | string | Oui | Valeur attendue : `trip_update`. |
| `source` | string | Oui | `simulation_java`, `simulation` ou `sncf_gtfs_rt`, selon le producteur. |
| `schema_version` | integer | Oui | Version canonique attendue : `1`. |
| `trip_id` | string | Oui | Identifiant de course dans le flux ; clé Kafka. Ce n’est pas l’identifiant d’un véhicule physique. |
| `route_id` | string | Oui | Identifiant de ligne dans le référentiel correspondant au parcours. Les exemples H/J/L/O/R ne limitent pas les valeurs réelles. |
| `stop_id` | string | Oui | Identifiant d’arrêt dans le référentiel correspondant au parcours. |
| `scheduled_timestamp` | string ISO-8601 UTC ou `null` | Clé oui | Horaire théorique ; dans le mapping réel, reconstitué par `time - delay`, sans lecture de l’horaire dans `stop_times.txt`. |
| `estimated_timestamp` | string ISO-8601 UTC ou `null` | Clé oui | Horaire estimé ; dans le mapping réel, issu du champ `time` de l’arrivée ou du départ retenu. |
| `delay_seconds` | integer signé | Oui | Écart en secondes : positif = retard ; zéro = écart nul ; négatif = avance. |
| `event_timestamp` | string ISO-8601 UTC | Oui | Instant d’observation. Dans le parcours réel, timestamp de publication de l’instantané GTFS-RT, pas heure de passage ni heure de rejeu. |
| `ingested_at` | string ISO-8601 UTC | Oui | Timestamp porté par le payload à sa génération/export. Conservé lors du rejeu JSONL ; ce n’est pas nécessairement l’heure de publication Kafka ni l’ingestion Bronze. |

Les timestamps non nuls du payload conforme sont exprimés en UTC avec le suffixe `Z`. Les identifiants sont des chaînes non vides ; ils ne doivent pas être remplacés par les noms de lignes ou d’arrêts.

Les clés `scheduled_timestamp` et `estimated_timestamp` appartiennent au format canonique. Pour la compatibilité du format, une valeur `null` signifie un horaire indisponible ; elle ne signifie jamais « zéro retard ». Le mapping réel actuel retient seulement des événements disposant explicitement de `time` et `delay` : il produit donc les deux horaires non nuls. Les producteurs de simulation nominaux les renseignent également.

### Trois horloges à distinguer

1. Observation métier : `event_timestamp` du payload. Le parcours réel utilise le timestamp du flux archivé.
2. Génération/export du payload : `ingested_at` dans le JSON. Sa valeur n’est pas remplacée par l’heure du rejeu.
3. Ingestion technique : Bronze ajoute son propre `ingested_at`, ainsi que `ingestion_date`, `kafka_timestamp`, topic, partition et offset.

Dans la transformation Python fournie, `ingested_at` reste la colonne héritée de Bronze ; le champ `_event.ingested_at` du JSON n’est pas sélectionné séparément dans Silver. Le timestamp du payload reste consultable dans `payload_json` en Bronze. Il ne faut donc pas présenter le champ Silver homonyme comme une conservation distincte du timestamp du producteur. Pour exploiter les deux en Silver, ajouter ultérieurement des colonnes explicitement nommées et les tester.

### Mapping SNCF GTFS-RT retenu

Le mapping documenté pour le parcours réel applique les choix suivants :

- Conserver les courses et mises à jour d’arrêt `SCHEDULED`.
- Exclure les courses `ADDED` et `CANCELED`, ainsi que les arrêts `SKIPPED`, avec des compteurs de conversion.
- Retenir en priorité l’arrivée si elle contient explicitement `time` et `delay` ; sinon essayer le départ avec ces deux champs explicites.
- Préserver un retard explicitement égal à zéro et les valeurs négatives ; ne pas confondre champ absent et valeur zéro.
- Affecter `estimated_timestamp` depuis le `time` choisi et reconstituer `scheduled_timestamp = time - delay`.
- Résoudre les identifiants avec les références GTFS statiques du lot ; enrichir les noms lors du traitement Silver.

Une exclusion de conversion avant Kafka n’est pas un rejet Silver. Les observations retenues ne couvrent pas toutes les catégories d’événements GTFS-RT ni une mesure exhaustive de la ponctualité.

### Identité des observations et rejeu

Dans le parcours réel, l’identifiant est déterministe à partir de l’observation et de ses caractéristiques. `ingested_at` n’intervient pas dans cette identité. Réexporter la même observation sans changement de ses caractéristiques doit conserver son identifiant ; un nouvel instantané ayant un autre timestamp d’observation constitue une nouvelle observation.

Cette propriété n’autorise pas à republier aveuglément un export. L’idempotence du producteur Kafka n’élimine pas les renvois applicatifs entre deux exécutions distinctes. La déduplication Silver est bornée par un watermark d’un jour sur `event_timestamp` et utilise `event_id` ; elle ne constitue pas une mémoire globale illimitée des identifiants.

Un nouvel instantané peut contenir une nouvelle observation du même passage. Les agrégats ne sélectionnent pas automatiquement la dernière prévision de chaque passage.

## Règles attendues du contrat

Un événement nominal conforme doit respecter les règles suivantes. Toutes ne sont pas encore contrôlées par Silver.

1. `event_id` est un UUID présent et non vide.
2. `event_type` vaut `trip_update`.
3. `schema_version` vaut `1`.
4. `source` correspond au producteur et au parcours choisis.
5. `trip_id`, `route_id` et `stop_id` sont présents, non vides et correspondent au périmètre de références.
6. `delay_seconds` est un entier signé compatible avec le schéma entier Spark ; une valeur négative est autorisée et représente une avance.
7. `event_timestamp` et `ingested_at` du payload sont des timestamps ISO-8601 UTC valides, sérialisés avec `Z`.
8. Les horaires non nuls sont des timestamps ISO-8601 UTC valides. Lorsque les deux sont renseignés :

   ```text
   estimated_timestamp - scheduled_timestamp = delay_seconds
   ```

9. La clé Kafka vaut `trip_id` pour le parcours nominal.
10. Le topic et la source sont cohérents : ne pas mélanger observations réelles et simulation.

Il n’existe pas de règle générale `delay_seconds >= 0`. Le signe du retard est une information métier à conserver, non une cause de rejet.

Le statut « conforme au contrat » est plus exigeant que « accepté par la validation Silver actuelle ». Par exemple, un message peut passer les contrôles Silver sans que son UUID, sa source ou sa version aient été vérifiés.

## Couverture actuelle de la validation Silver

La couverture ci-dessous reprend la transformation et l’inspection Python fournies, ainsi que les comportements documentés du pipeline. Elle doit être revalidée si ces fichiers changent : ce contrat n’ajoute aucun contrôle au code.

### Validation événementielle

`parse_and_validate_trip_updates` :

- Parse `payload_json` avec le schéma Spark et exploite `_corrupt_record` pour les erreurs signalées par le parseur.
- Vérifie `event_id` et `trip_id` : non nuls et non vides après examen avec `trim`.
- Convertit `event_timestamp` et vérifie qu’il n’est pas nul après conversion.
- Vérifie `delay_seconds` : non nul après parsing avec le type entier.
- Calcule `delay_minutes = delay_seconds / 60.0`.
- Convertit les horaires en timestamps, sans contrôle dédié de leur présence ni de leur cohérence avec le retard.

La première erreur détectée est conservée dans `validation_error`, selon cet ordre :

| Priorité | Code | Condition vérifiée |
|---:|---|---|
| 1 | `invalid_json_or_schema` | `_corrupt_record` est renseigné. |
| 2 | `missing_event_id` | Identifiant d’événement nul ou vide après `trim`. |
| 3 | `missing_trip_id` | Identifiant de course nul ou vide après `trim`. |
| 4 | `invalid_or_missing_event_timestamp` | Timestamp d’observation nul après conversion. |
| 5 | `invalid_or_missing_delay_seconds` | Retard nul après parsing entier. |

Ce mécanisme ne garantit ni un diagnostic de toutes les erreurs d’un message ni une validation stricte de l’ensemble du JSON contre le contrat. Un type incompatible peut déjà entraîner une erreur du parseur, avant les contrôles de champs.

Les événements classés invalides sont conservés avec leur payload, leurs métadonnées Kafka, leur motif et leur timestamp de rejet :

```text
Simulation : data/lakehouse/silver/rejected_events
Réel       : data/lakehouse_real/silver/rejected_events
```

Les événements sans erreur de validation suivent la déduplication avec watermark, puis l’enrichissement. Les doublons éliminés par la déduplication ne sont pas des rejets de validation. Le compteur `rejected_events` ne représente donc pas toutes les observations éventuellement absentes du résultat final, notamment les exclusions avant Kafka.

### Inspection bloquante avant Gold

`inspect_silver.py` vérifie :

- L’absence de doublons d’`event_id` dans la table des événements valides.
- Les champs requis effectivement contrôlés : `event_id` et `trip_id` non nuls/non vides, `event_timestamp` non nul et `delay_seconds` non nul.
- La cohérence de `delay_minutes` avec `delay_seconds / 60.0`, avec une tolérance de `1e-9`, et l’absence de minutes nulles ou `NaN`.
- La complétude de l’enrichissement via `check_enrichment` : `route_id`, `stop_id`, `line_name` et `stop_name` doivent être non nuls et non vides après suppression des espaces en début et fin (`trim`).

Une anomalie de ces contrôles bloque la suite du pipeline avant les agrégations Gold et la publication PostgreSQL.

`check_enrichment` affiche jusqu’à dix lignes incomplètes avec `event_id`, les identifiants et les noms concernés, puis lève une `RuntimeError` contenant `enrichissement incomplet`. Si aucune ligne n’est incomplète, il affiche `Contrôle de l'enrichissement : OK`.

Ce contrôle vérifie la complétude des quatre champs dans les données Silver inspectées. Il ne valide pas la correspondance sémantique de chaque nom avec son identifiant, la version du référentiel ni la fraîcheur des références. Un nom non vide mais incorrect ne sera pas détecté par ce seul contrôle.

Une anomalie d’enrichissement ne crée pas de `validation_error` supplémentaire et ne déplace pas automatiquement l’observation vers `rejected_events`. L’événement peut avoir passé la validation événementielle et être déjà écrit dans `trip_delays` ; c’est alors l’inspection qui bloque les traitements suivants. Les lignes existantes ne sont ni corrigées ni supprimées par cette inspection.

### Règles non garanties par les contrôles Silver actuels

| Règle attendue | Couverture actuelle |
|---|---|
| Format UUID de `event_id` | Non vérifié explicitement. |
| Valeurs de `event_type`, `source`, `schema_version` | Non vérifiées explicitement comme valeurs métier attendues. |
| `source` présente et non vide | Pas de contrôle dédié. La présence non nulle/non vide de `route_id` et `stop_id` est désormais vérifiée par `check_enrichment` lors de l’inspection. |
| Correspondance sémantique des noms avec les identifiants GTFS | Non garantie par le contrôle de complétude. `line_name` et `stop_name` sont contrôlés non nuls/non vides, sans comparaison dédiée à un référentiel de référence. |
| Validité du `ingested_at` contenu dans le JSON | Pas de contrôle dédié ; ne pas le confondre avec l’ingestion Bronze. |
| Présence explicite du fuseau UTC et du suffixe `Z` | La conversion Spark ne constitue pas ce contrôle strict. |
| Présence et validité des deux horaires | Conversion, mais pas de contrôle de conformité dédié. |
| Cohérence horaires / `delay_seconds` | Pas de contrôle Silver dédié. |
| Égalité clé Kafka / `trip_id` et cohérence topic / source | Pas de contrôle dédié dans la transformation décrite. |
| Rejet métier des versions inconnues | Non garanti par la seule présence de `schema_version`. |

Les retards négatifs ne figurent pas parmi les anomalies manquantes : ils sont autorisés par le contrat.

## Exemple métier

```text
Course : TRAIN-H-005
Ligne : H
Arrêt : stop_87271010
Horaire prévu : 14:30 UTC
Horaire estimé : 14:30 UTC
Écart : 0 seconde = 0 minute
```

Une avance doit également conserver son signe :

```text
Horaire prévu : 14:30 UTC
Horaire estimé : 14:28 UTC
Écart : -120 secondes = -2 minutes
```

Un retard est considéré comme significatif lorsque :

```text
delay_seconds > 300
```

Cela correspond à strictement plus de cinq minutes de retard. Un retard de 300 secondes exactement n’est pas significatif ; une avance non plus.

### Sémantique des agrégats Gold

- `event_count` compte des observations, pas des trains uniques.
- `delayed_event_count` compte les observations d’arrêt dont le retard est strictement supérieur à cinq minutes, par arrêt et par jour.
- `delayed_trip_count` compte les courses distinctes avec au moins une observation au-delà du seuil, par ligne et par fenêtre. Sa somme sur plusieurs fenêtres n’est pas un nombre de trains uniques sur toute la période.
- `event_date` et les fenêtres de 15 minutes sont calculés depuis `event_timestamp`, en UTC pour les traitements configurés ainsi. Ils représentent la date d’observation du flux, pas nécessairement celle du passage.
- Les moyennes portent sur les écarts signés conservés : ne pas remplacer les avances par zéro sans décision métier et modification explicites des traitements.
- Une moyenne recomposée sur plusieurs agrégats doit être pondérée par `event_count`. Les moyennes Gold étant arrondies, cette recomposition peut être légèrement approximative.
- Les tables par ligne/fenêtre et par arrêt/jour représentent les mêmes observations à des granularités différentes : ne pas additionner leurs volumes.

Ces indicateurs décrivent les observations sélectionnées, pas la ponctualité finale du réseau ni une couverture continue de la période.

## Modèles de simulation actuels

Les cinq modèles Java documentés sont :

| `trip_id` | `route_id` | `stop_id` | Retard |
|---|---|---|---:|
| `TRAIN-O-001` | `O` | `stop_87271007` | 480 secondes |
| `TRAIN-J-002` | `J` | `stop_87271003` | 720 secondes |
| `TRAIN-R-003` | `R` | `stop_87271005` | 120 secondes |
| `TRAIN-L-004` | `L` | `stop_87113001` | 600 secondes |
| `TRAIN-H-005` | `H` | `stop_87271010` | 0 seconde |

Les fichiers de référence correspondants sont :

```text
spark/resources/simulation/routes.csv
spark/resources/simulation/stops.csv
```

Ces libellés sont simulés et ne représentent pas de vrais noms de lignes ou de gares SNCF. Ce tableau décrit les modèles Java, pas une garantie d’identité avec les modèles du simulateur Python complémentaire.

Le parcours réel utilise les références préparées dans `data/reference/real/<lot>/`, avec `routes.csv`, `stops.csv` et le manifeste de provenance. Le dossier historique `data/reference/real/sncf` reste distinct des dossiers de collecte. Les chemins effectifs doivent correspondre au lot traité ; un changement de référentiel ne réenrichit pas automatiquement les lignes déjà écrites dans Silver.

## Évolution du contrat

`schema_version` décrit la version du payload, pas la version du flux GTFS ni celle du référentiel. Sa présence prépare l’évolution du format, mais le contrôle métier des versions acceptées reste à implémenter dans Silver.

Lorsqu’une modification incompatible sera nécessaire :

1. Incrémenter `schema_version` et définir les versions acceptées ou rejetées.
2. Documenter les champs, types, valeurs autorisées et nouvelles sémantiques.
3. Mettre à jour le schéma Spark et les contrôles de validation concernés.
4. Ajouter ou modifier les tests Java et Python, y compris les cas invalides et les versions inconnues.
5. Vérifier la compatibilité entre producteurs, Bronze, Silver, Gold et PostgreSQL, ainsi que les impacts sur les checkpoints et les données existantes avant tout déploiement.

Cette documentation décrit le format canonique `1`, les choix du mapping réel et le contrôle de complétude `check_enrichment` ajouté à l’inspection Silver. Aucun champ du payload n’est modifié par ce contrôle ; cette mise à jour documentaire n’ajoute pas de modification de code supplémentaire. Les tests d’intégration, les résultats d’échantillons et la reprise sans nouvelle publication sont documentés dans le README et le runbook : ils ne remplacent pas les contrôles du contrat et ne constituent pas une garantie universelle d’exactly-once.
