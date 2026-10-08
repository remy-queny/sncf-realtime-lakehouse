# Source SNCF réelle — Validation locale

## Périmètre

Le projet dispose de deux parcours séparés :
- Simulation : événements et référentiels simulés.
- Réel : événements normalisés depuis un instantané SNCF GTFS-RT,
  enrichis avec un référentiel GTFS statique.

Le parcours réel validé est :

Instantané GTFS-RT téléchargé manuellement
→ décodage et mapping Java
→ export JSONL
→ publication Java dans Kafka
→ Bronze Delta
→ Silver Delta enrichie
→ Gold Delta
→ PostgreSQL
→ Metabase.

Les traitements sont exécutés à la demande.
Le téléchargement automatique HTTP par Java n'est pas encore implémenté.

## Sources utilisées

- GTFS-RT Trip Updates :
  https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates
- GTFS statique :
  https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip

Les identifiants sont conservés tels quels.
La ligne est retrouvée via la correspondance
trip_id → trips.txt → route_id → routes.txt.

Les noms d'arrêt proviennent de stops.txt.

Le manifeste des références préparées conserve l'empreinte SHA-256
de l'archive et la date de préparation.

## Instantané inspecté

Validation réalisée le 8 octobre 2026.

- Version GTFS-RT : 1.0.
- Timestamp du flux : 2026-10-08T17:18:01Z.
- Courses : 2 001.
- Mises à jour d'arrêt : 17 869.
- Courses SCHEDULED : 1 776, toutes résolues dans le référentiel.
- Courses ADDED : 196.
- Courses CANCELED : 29.

Référentiel statique :
- 60 614 courses.
- 744 lignes.
- 8 883 arrêts.
- Aucun identifiant dupliqué dans ces trois tables.

## Mapping et exclusions

Le premier périmètre accepte les courses et arrêts SCHEDULED.

- Courses ADDED et CANCELED exclues des agrégats :
  838 mises à jour d'arrêt concernées.
- Arrêts SKIPPED exclus : 53.
- Événements canoniques exportés : 16 978.
- event_id distincts dans cet export : 16 978.

Le bilan couvre toutes les mises à jour d'arrêt :
17 869 = 838 + 53 + 16 978.

La priorité est donnée à l'arrivée lorsqu'elle fournit time et delay.
Sinon, le départ est utilisé s'il fournit ces deux champs.

- estimated_timestamp : time sélectionné.
- scheduled_timestamp : time - delay, horaire reconstitué.
- delay_seconds : delay sélectionné.
- event_timestamp : timestamp de publication du flux.
- source : sncf_gtfs_rt.

L'identifiant déterministe exclut ingested_at.
Une nouvelle observation avec un autre timestamp de flux produit
un nouvel identifiant.

Les instantanés Protobuf sont conservés séparément des tables Bronze.
Bronze reçoit le JSON canonique publié dans Kafka.

## Isolation de la simulation

| Composant | Simulation | Réel |
|---|---|---|
| Topic | sncf.trip_updates.raw | sncf.trip_updates.real.raw |
| Lakehouse | data/lakehouse | data/lakehouse_real |
| Checkpoints | data/checkpoints | data/checkpoints_real |
| Références | spark/resources/simulation | data/reference/real/sncf |
| Schéma PostgreSQL | public | sncf_real |

La publication PostgreSQL refuse les couples lakehouse/schéma
non autorisés.

Les données générées, instantanés et checkpoints ne sont pas versionnés.

## Échantillon validé de bout en bout

Seuls les 10 premiers événements de l'export ont été publiés dans Kafka.

Résultats :
- Bronze : 10 événements, offsets 0 à 9.
- Silver : 10 événements uniques, aucun rejet.
- Tous les noms de lignes et d'arrêts sont renseignés.
- Gold par ligne : 1 agrégat contenant 10 événements.
- Gold par arrêt : 10 agrégats contenant chacun 1 événement.
- PostgreSQL : mêmes valeurs dans le schéma sncf_real.
- Metabase : deux graphiques sur un dashboard réel distinct.

Ligne : Lux/Alsace/Loraine - Paca.
Fenêtre d'observation UTC : 17:15 à 17:30.
Retard moyen : 1,5 minute.
Retard maximal : 5 minutes.

Colmar, Mulhouse et Belfort - Montbéliard TGV présentent 5 minutes.
Les sept autres arrêts présentent 0 minute.

Le seuil significatif est strictement supérieur à 5 minutes :
les compteurs de retard significatif sont donc tous à zéro.

## Limites

- Les 16 978 événements ont été exportés, mais seuls 10 ont parcouru
  toute la chaîne jusqu'au dashboard.
- L'échantillon concerne les arrêts d'une seule course :
  il ne mesure pas la ponctualité globale de la ligne.
- Les fenêtres utilisent le timestamp du flux, pas l'heure de passage.
- Le dashboard n'est pas alimenté en continu ni provisionné automatiquement.
- Le téléchargement de la source et du référentiel reste manuel.
- Les alertes et annulations ne sont pas restituées comme indicateurs métier.
- L'UUID déterministe et l'idempotence Kafka ne garantissent pas
  un exactly-once universel.