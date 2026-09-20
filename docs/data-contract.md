# Contrat de données — Trip Update

## Objectif

Ce document définit le format d'un événement de retard ferroviaire envoyé dans Kafka.

Ce même format sera utilisé par :

- Le simulateur Python, au début du projet.
- Le producteur Java, plus tard.
- Les traitements PySpark.
- Les couches Bronze, Silver et Gold du lakehouse.

## Topic Kafka

```text
sncf.trip_updates.raw
```

## Clé Kafka

La clé Kafka est :

```text
trip_id
```

Tous les événements d'un même train utilisent la même clé.

## Exemple d'événement

```json
{
  "event_id": "a4f8f3b2-1fe8-4f73-bb19-40faef95b98d",
  "event_type": "trip_update",
  "source": "simulation",
  "schema_version": 1,
  "trip_id": "TRAIN-H-001",
  "route_id": "H",
  "stop_id": "stop_87271007",
  "scheduled_timestamp": "2026-09-20T14:30:00Z",
  "estimated_timestamp": "2026-09-20T14:38:00Z",
  "delay_seconds": 480,
  "event_timestamp": "2026-09-20T14:25:00Z",
  "ingested_at": "2026-09-20T14:25:02Z"
}
```

## Description des champs

| Champ | Type | Obligatoire | Description |
|---|---|---:|---|
| `event_id` | string UUID | Oui | Identifiant unique de l'événement. Il servira à la déduplication. |
| `event_type` | string | Oui | Type de l'événement. Valeur attendue : `trip_update`. |
| `source` | string | Oui | Source de l'événement : `simulation` au départ, puis `sncf_gtfs_rt`. |
| `schema_version` | integer | Oui | Version du format de données. La première version vaut `1`. |
| `trip_id` | string | Oui | Identifiant stable du train ou de la course. C'est la clé Kafka. |
| `route_id` | string | Oui | Identifiant de la ligne, par exemple `H`, `J` ou `R`. |
| `stop_id` | string | Oui | Identifiant de la gare ou de l'arrêt. |
| `scheduled_timestamp` | timestamp ISO-8601 UTC ou `null` | Oui | Heure théorique de passage du train. |
| `estimated_timestamp` | timestamp ISO-8601 UTC ou `null` | Oui | Heure estimée de passage du train. |
| `delay_seconds` | integer | Oui | Retard en secondes. Une valeur positive indique un retard. |
| `event_timestamp` | timestamp ISO-8601 UTC | Oui | Instant auquel l'information métier est observée. |
| `ingested_at` | timestamp ISO-8601 UTC | Oui | Instant auquel le producteur envoie l'événement dans Kafka. |

## Règles de validation

Un événement est valide si :

1. `event_id` est présent et non vide.
2. `event_type` vaut `trip_update`.
3. `schema_version` vaut `1`.
4. `trip_id`, `route_id` et `stop_id` sont présents et non vides.
5. `delay_seconds` est un nombre entier.
6. `event_timestamp` et `ingested_at` sont des dates ISO-8601 valides.
7. Si les deux horaires existent, alors :

   ```text
   estimated_timestamp - scheduled_timestamp = delay_seconds
   ```

## Exemple métier

```text
Train : TRAIN-H-001
Ligne : H
Gare : stop_87271007
Horaire prévu : 14:30 UTC
Horaire estimé : 14:38 UTC
Retard : 480 secondes = 8 minutes
```

Un retard est considéré comme significatif lorsque :

```text
delay_seconds > 300
```

Cela correspond à plus de cinq minutes de retard.