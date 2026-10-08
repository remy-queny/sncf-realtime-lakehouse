# Contrat de données — Trip Update

## Objectif

Ce contrat décrit le payload JSON publié dans Kafka par le producteur
Java en mode simulation. Le simulateur Python complémentaire utilise
également ce format.

Bronze conserve le payload original. Silver le transforme en colonnes
typées et enrichies. Gold contient des agrégats, pas des événements au
même format JSON.

## Topic Kafka

```text
sncf.trip_updates.raw
```

## Clé Kafka

La clé Kafka est :

```text
trip_id
```

Tous les événements d'une même course utilisent la même clé Kafka afin
de conserver leur ordre dans la partition correspondante.

## Exemple d'événement

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

Cet exemple est fictif et correspond aux identifiants du modèle H actuel.
À chaque lancement, le producteur génère un nouvel UUID et des timestamps
actuels. Les noms de lignes et d'arrêts sont ajoutés dans Silver à partir
des référentiels de simulation.

## Description des champs

| Champ | Type | Obligatoire | Description |
|---|---|---:|---|
| `event_id` | string UUID | Oui | Identifiant unique de l'événement, utilisé pour la déduplication. |
| `event_type` | string | Oui | Type de l'événement. Valeur attendue : `trip_update`. |
| `source` | string | Oui | `simulation_java` pour le producteur Java actuel ; source réelle à définir lors de son intégration. |
| `schema_version` | integer | Oui | Version du format de données. La première version vaut `1`. |
| `trip_id` | string | Oui | Identifiant stable du train ou de la course. Il sert de clé Kafka. |
| `route_id` | string | Oui | Identifiant de la ligne, par exemple `H`, `J`, `L`, `O` ou `R`. |
| `stop_id` | string | Oui | Identifiant de l'arrêt ou de la gare. |
| `scheduled_timestamp` | timestamp ISO-8601 UTC ou `null` | Oui | Heure théorique de passage de la course. |
| `estimated_timestamp` | timestamp ISO-8601 UTC ou `null` | Oui | Heure estimée de passage de la course. |
| `delay_seconds` | integer | Oui | Retard en secondes. Une valeur positive indique un retard ; zéro indique aucun retard. |
| `event_timestamp` | timestamp ISO-8601 UTC | Oui | Instant auquel l'information métier est observée. |
| `ingested_at` | timestamp ISO-8601 UTC | Oui | Instant auquel le producteur envoie l'événement dans Kafka. |

Les timestamps doivent être exprimés en UTC avec le suffixe `Z` lorsqu'ils
sont sérialisés dans le payload. Les champs horaires marqués comme
obligatoires doivent être présents dans un événement conforme ; leurs
valeurs peuvent être `null` uniquement lorsque le contrat l'autorise
explicitement.

## Règles attendues du contrat

Un événement conforme au contrat doit respecter les règles suivantes.
Toutes ne sont pas encore contrôlées par Silver.

1. `event_id` est présent et non vide.
2. `event_type` vaut `trip_update`.
3. `schema_version` vaut `1`.
4. `trip_id`, `route_id` et `stop_id` sont présents et non vides.
5. `delay_seconds` est un nombre entier.
6. `event_timestamp` et `ingested_at` sont des timestamps ISO-8601 valides.
7. Si `scheduled_timestamp` et `estimated_timestamp` existent, alors :

   ```text
   estimated_timestamp - scheduled_timestamp = delay_seconds
   ```

8. `delay_seconds` est supérieur ou égal à zéro pour un retard classique.

## Couverture actuelle de la validation Silver

La transformation Silver contrôle actuellement :

- Les erreurs de parsing JSON ou de conformité au schéma Spark.
- `event_id` et `trip_id` : non nuls et non vides après suppression des espaces en début et fin.
- `event_timestamp` : convertible en timestamp.
- `delay_seconds` : non nul après parsing avec le type entier.
- Le calcul `delay_minutes = delay_seconds / 60.0`.

La première erreur détectée est enregistrée dans `validation_error`.
Les événements invalides sont conservés dans :

```text
data/lakehouse/silver/rejected_events
```

Ne sont pas encore contrôlés explicitement :

- Le format UUID de `event_id`.
- La valeur attendue de `event_type`.
- La valeur attendue de `schema_version`.
- La présence non vide de `source`, `route_id` et `stop_id`.
- La validité de `ingested_at` fourni dans le payload.
- La présence explicite du fuseau UTC dans les timestamps.
- La cohérence entre les horaires et `delay_seconds`.
- L'interdiction des retards négatifs.

L'inspection de la table Silver contrôle également :

- Les doublons d'`event_id`.
- Les champs obligatoires retenus.
- La cohérence entre `delay_seconds` et `delay_minutes`.
- La présence des identifiants de ligne et d'arrêt à enrichir.

Cette inspection s'exécute avant les calculs Gold et la publication
PostgreSQL.

## Exemple métier

```text
Course : TRAIN-H-005
Ligne : H
Arrêt : stop_87271010
Horaire prévu : 14:30 UTC
Horaire estimé : 14:30 UTC
Retard : 0 seconde = 0 minute
```

Un retard est considéré comme significatif lorsque :

```text
delay_seconds > 300
```

Cela correspond à strictement plus de cinq minutes de retard.

Un retard de exactement 300 secondes, soit 5 minutes, n'est donc pas
compté comme un retard significatif dans les agrégats Gold.

## Modèles de simulation actuels

Les cinq modèles Java actuellement disponibles sont :

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

Ces libellés sont simulés et ne représentent pas de vrais noms de lignes
ou de gares SNCF.

## Évolution du contrat

Le champ `schema_version` permet de faire évoluer le contrat sans
interpréter les nouvelles versions comme la version actuelle.

Lorsqu'une modification incompatible sera nécessaire :

1. Incrémenter `schema_version`.
2. Documenter les nouveaux champs ou changements de type.
3. Mettre à jour le schéma Spark.
4. Ajouter ou modifier les tests Java et Python.
5. Vérifier la compatibilité entre Bronze, Silver, Gold et PostgreSQL.

La connexion à une source GTFS-RT réelle devra conserver ce contrat
canonique ou utiliser une étape de normalisation avant la publication
dans Kafka.