# SNCF Real-Time Lakehouse

Projet local de data engineering pour normaliser des observations SNCF GTFS-RT, les traiter dans un lakehouse Delta et restituer des indicateurs de retard dans Tableau.

> État au 27 octobre 2026 : parcours à la demande validé sur 110 observations réelles. Le mode quasi-temps réel fait partie des objectifs restants de la V1 ; il n’est pas encore implémenté. Airflow reste prévu en V2.

## Architecture

```text
Java → Kafka → PySpark → Delta Lake Bronze / Silver / Gold → PostgreSQL → Tableau
```

- Bronze : payload JSON original et métadonnées Kafka.
- Silver : validation, déduplication, enrichissement GTFS et traçabilité des rejets.
- Gold : agrégats par ligne/fenêtre de 15 minutes et par arrêt/jour.
- PostgreSQL : publication des agrégats pour la BI ; Tableau utilise deux sources distinctes.

Stack : Java 17, Kafka 4.1.0, PySpark 3.5.3, Delta Lake 3.2.0, PostgreSQL 17, Tableau Desktop, Docker Compose, JUnit, pytest et GitHub Actions. Exécution locale sur Mac, sans dépendance cloud.

## Dashboards Tableau

### Synthèse des retards

Trois KPI et deux graphiques : observations analysées, retard moyen pondéré, retard maximal, classements par ligne et par arrêt.

![Synthèse Tableau des retards SNCF](docs/screenshots/tableau-real-overview.png)

### Analyse des arrêts

Volume, retard moyen pondéré, part des observations au-delà de cinq minutes, maximum et filtre de période.

![Analyse détaillée des arrêts dans Tableau](docs/screenshots/tableau-real-station-analysis.png)

Les captures sont statiques ; les filtres se manipulent dans Tableau Desktop. Vérifier que les images versionnées correspondent à la démonstration à 110 observations. La connexion en direct à PostgreSQL n’implique pas une actualisation automatique de l’affichage.

## Résultats validés

| Date d’observation | Observations | Observations > 5 min | Part > 5 min |
|---|---:|---:|---:|
| 8 octobre 2026 | 10 | 0 | 0 % |
| 15 octobre 2026 | 100 | 39 | 39 % |
| Total | 110 | 39 | 35,45 % |

- PostgreSQL : 7 agrégats ligne/fenêtre et 84 agrégats arrêt/jour ; chaque table représente les mêmes 110 observations.
- Tableau sur les deux lots : moyenne pondérée affichée de 7,68 minutes et maximum de 45 minutes.
- Reprise réelle du 20 octobre : captures Silver/PostgreSQL identiques avant et après une relance sans nouvel envoi Kafka ; 110 observations Silver et 0 rejet dans ce contrôle.
- Qualité du 27 octobre : 14 nouveaux cas d’enrichissement réussis, puis suite Python complète et inspection réelle confirmées sans erreur.

Le premier instantané a produit 16 978 événements normalisés/exportés ; seuls 10 de ce lot, puis 100 d’un second lot, ont été validés de bout en bout. L’échantillon ne représente pas la ponctualité globale du réseau. Les compteurs portent sur des observations, pas sur des trains uniques ; le seuil est strictement supérieur à cinq minutes.

## Utilisation

Prérequis : Docker Desktop, JDK 17, Maven, Python 3.12 ; Tableau Desktop et son pilote PostgreSQL pour la BI.

Pour une installation complète, suivre le [runbook](docs/runbook.md). Depuis la racine d’une installation déjà préparée :

```bash
docker compose up -d kafka kafka-ui postgres
```

Après vérification des services, avec topic, tables, environnement Python et références déjà prêts :

```bash
bash scripts/run_real_pipeline.sh \
  data/reference/real/collection-eYyLYD
```

Le chemin ci-dessus est celui du lot historique : utiliser le dossier de références du lot traité. Il n’est pas recréé par Git. Cette commande traite les données déjà disponibles dans Kafka ; elle ne collecte ni ne publie de nouveaux événements. Actualiser ensuite les deux sources dans Tableau.

Le mode simulation reste disponible séparément : `run_demo.sh` produit un événement et lance le traitement ; `run_pipeline.sh` traite sans nouvel envoi. Ne pas mélanger les deux parcours :

| Périmètre | Simulation | Réel |
|---|---|---|
| Topic | `sncf.trip_updates.raw` | `sncf.trip_updates.real.raw` |
| Delta | `data/lakehouse` | `data/lakehouse_real` |
| Checkpoints | `data/checkpoints` | `data/checkpoints_real` |
| Schéma PostgreSQL | `public` | `sncf_real` |

## Fiabilité et limites

- Les contrôles Silver bloquent avant Gold : doublons, champs requis retenus, conversion secondes/minutes et complétude de `route_id`, `stop_id`, `line_name`, `stop_name`.
- La complétude ne prouve pas l’exactitude sémantique des noms GTFS ; une anomalie d’enrichissement n’est pas automatiquement déplacée vers les rejets.
- Bronze/Silver utilisent actuellement `availableNow=True` : traitement des données disponibles puis arrêt. Gold et sa publication restent à la demande.
- Les checkpoints et upserts permettent les scénarios de reprise testés ; la déduplication est bornée par watermark, sans garantie universelle d’exactly-once.
- Le publisher est limité à 10 000 agrégats par table. Les lignes disparues de Gold ne sont pas automatiquement supprimées de PostgreSQL.
- Metabase reste historique et encore présent dans Compose. Les alertes, la DLQ Kafka et Airflow ne sont pas implémentés.
- Ne pas versionner `.env`, secrets, données Delta, checkpoints, exports volumineux ou captures brutes sans revue.

## Documentation

- [Contrat de données](docs/data-contract.md) : format, mapping réel et couverture des contrôles.
- [Runbook](docs/runbook.md) : installation, exécution, reprise et dépannage.
- [Guide Tableau](bi/tableau/README.md) : connexion, calculs, filtres et réouverture.
- [Validation historique de la source réelle](docs/real-source.md) : premier instantané et résultats d’origine.

## Avancement

### Réalisé

- [x] Infrastructure locale Kafka, Kafka UI et PostgreSQL avec Docker Compose.
- [x] Producteur Java et simulateur Python, avec cas invalides.
- [x] Décodage GTFS-RT, mapping canonique et export JSONL réel.
- [x] Composant HTTP Java et script de collecte/préparation à la demande.
- [x] Références GTFS réelles avec manifeste de provenance.
- [x] Isolation simulation/réel : topics, Delta, checkpoints et schémas PostgreSQL.
- [x] Bronze/Silver incrémentaux, validation, déduplication et rejets traçables.
- [x] Contrôles Silver bloquants, dont complétude de l’enrichissement et 14 nouveaux cas testés.
- [x] Agrégats Gold et publication PostgreSQL par upsert avec garde-fou de périmètre.
- [x] Scripts de démonstration, traitements de simulation et traitement réel à la demande.
- [x] Validation des 110 observations réelles jusqu’à PostgreSQL et Tableau.
- [x] Deux dashboards Tableau, sources distinctes, moyennes pondérées et filtre testé.
- [x] Actualisation manuelle Tableau après nouvelle publication : passage de 10 à 110 observations.
- [x] Captures d’état paramétrables et reprise normale sans nouvel événement validée en simulation et en réel.
- [x] Tests Java/Python et CI GitHub Actions existants ; suites locales revalidées selon les bilans documentés.
- [x] Contrat, runbook et README actualisés ; scénario de simulation validé depuis un clone isolé.

### À faire — V1 quasi-temps réel

- [ ] Examiner les composants actuels et définir le mode actif sans casser le mode à la demande.
- [ ] Définir les paramètres, le volume et la cadence après examen de la source et des ressources locales.
- [ ] Automatiser collecte et publication des nouveaux instantanés, avec références compatibles et erreurs gérées.
- [ ] Éviter les renvois d’instantanés inchangés et tester la reprise d’une publication partielle.
- [ ] Maintenir Bronze/Silver actifs en micro-batches avec arrêt propre et contrôles qualité adaptés.
- [ ] Actualiser automatiquement Gold/PostgreSQL avec des cumuls corrects, des retries maîtrisés et un coût mesuré.
- [ ] Mesurer fraîcheur source → PostgreSQL et délai collecteur → PostgreSQL ; détecter une source stagnante.
- [ ] Tester la compatibilité des checkpoints et la reprise du mode actif, sans effacer l’historique.
- [ ] Démontrer plusieurs instantanés récents traités sans commande intermédiaire, puis vérifier Tableau après actualisation manuelle.
- [ ] Vérifier le parcours réel depuis un clone isolé et archiver les preuves, rapports de tests et résultat CI du commit retenu.
- [ ] Confiner les ports à l’interface locale et créer un compte BI en lecture seule.
- [ ] Vérifier le classeur Tableau, sa réouverture et l’absence de secrets avant versionnement.
- [ ] Actualiser les captures du README, vérifier le guide Tableau et enregistrer une démonstration propre.
- [ ] Retirer Metabase et ses références inutiles sans supprimer les volumes métier.
- [ ] Compléter architecture, documentation et qualité de code ; acter les contrôles du contrat, alertes et DLQ retenus pour la clôture.
- [ ] Ajouter des analyses temporelles seulement après validation d’un historique suffisant, sans en faire une preuve de streaming à elles seules.

### À faire — V2 et évolutions

- [ ] Ajouter Airflow pour planifier les références GTFS, les contrôles et les recalculs/publications batch.
- [ ] Documenter une éventuelle trajectoire Azure, optionnelle et non bloquante.
