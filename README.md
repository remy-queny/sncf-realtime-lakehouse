# SNCF Real-Time Lakehouse

Projet local de data engineering pour normaliser des observations SNCF GTFS-RT, les traiter dans un lakehouse Delta et restituer des indicateurs de retard dans Tableau.

> État au 27 octobre 2026 : parcours à la demande validé sur 110 observations réelles ; mode actif supervisé démontré en simulation jusqu’à PostgreSQL sur 4 observations. La collecte SNCF périodique et le parcours réel actif restent à implémenter et valider. Airflow reste prévu en V2.

## Architecture

```text
Java → Kafka → PySpark → Delta Lake Bronze / Silver / Gold → PostgreSQL → Tableau
```

- Bronze : payload JSON original et métadonnées Kafka.
- Silver : validation, déduplication, enrichissement GTFS et traçabilité des rejets.
- Gold : agrégats par ligne/fenêtre de 15 minutes et par arrêt/jour.
- PostgreSQL : publication des agrégats pour la BI ; Tableau utilise deux sources distinctes.

Deux modes coexistent : à la demande, Bronze/Silver utilisent `availableNow=True` ; en mode actif de simulation, ils restent en micro-batches et un cycle périodique actualise Gold/PostgreSQL à partir d’une version Silver figée.

Stack : Java 17, Kafka 4.1.0, PySpark 3.5.3, Delta Lake 3.2.0, PostgreSQL 17, Tableau Desktop, Docker Compose, JUnit, pytest et GitHub Actions. Exécution locale sur Mac, sans dépendance cloud.

## Dashboards Tableau

### Synthèse des retards

Trois KPI et deux graphiques : observations analysées, retard moyen pondéré, retard maximal, classements par ligne et par arrêt.

![Synthèse Tableau des retards SNCF](docs/screenshots/tableau-real-overview.png)

### Analyse des arrêts

Volume, retard moyen pondéré, part des observations au-delà de cinq minutes, maximum et filtre de période.

![Analyse détaillée des arrêts dans Tableau](docs/screenshots/tableau-real-station-analysis.png)

Les captures concernent le parcours réel historique, pas le test actif de simulation. Vérifier qu’elles correspondent à la démonstration à 110 observations. Les filtres se manipulent dans Tableau Desktop ; une connexion directe à PostgreSQL n’implique pas une actualisation automatique de l’affichage.

## Résultats validés

### Données SNCF réelles — à la demande

| Date d’observation | Observations | Observations > 5 min | Part > 5 min |
|---|---:|---:|---:|
| 8 octobre 2026 | 10 | 0 | 0 % |
| 15 octobre 2026 | 100 | 39 | 39 % |
| Total | 110 | 39 | 35,45 % |

- PostgreSQL : 7 agrégats ligne/fenêtre et 84 agrégats arrêt/jour ; chaque table représente les mêmes 110 observations.
- Tableau sur les deux lots : moyenne pondérée affichée de 7,68 minutes et maximum de 45 minutes.
- Reprise réelle du 20 octobre : contenu Silver/PostgreSQL identique avant et après une relance sans nouvel envoi Kafka ; 110 observations Silver et 0 rejet dans ce contrôle.
- Qualité du 27 octobre : 14 cas ciblés d’enrichissement réussis, puis suite Python et inspection réelle confirmées sans erreur.

Le premier instantané a produit 16 978 événements normalisés/exportés ; seuls 10 de ce lot, puis 100 d’un second lot, ont été validés de bout en bout. L’échantillon ne représente pas la ponctualité globale du réseau. Les compteurs portent sur des observations, pas sur des trains uniques ; le seuil est strictement supérieur à cinq minutes.

### Simulation — mode actif supervisé

- Bronze/Silver actifs et propagation d’un nouvel événement jusqu’à PostgreSQL sans recalcul manuel : passage de 3 à 4 observations.
- État final du test : 4 agrégats et 4 observations dans chacune des deux tables de `sncf_active_test` ; ne pas additionner leurs volumes.
- Reprise Bronze/Silver avec les mêmes checkpoints : aucune nouvelle ligne écrite sans nouvel envoi, puis traitement d’un événement supplémentaire.
- Inspection Silver intermédiaire : 3 observations, 0 rejet et quatre contrôles qualité réussis ; contrôles également réussis dans les cycles Gold suivants.
- Republication à Gold inchangé : contenu complet des deux tables PostgreSQL identique avant/après.
- Supervision commune : démarrage des trois jobs, arrêt par un seul `Ctrl+C`, sortie inattendue de Silver testée sous supervision Bronze/Silver, puis sortie inattendue de Gold testée sous supervision des trois jobs.
- Arrêt normal du superviseur en code `0` ; perte d’un job signalée en code `1`, même si ce job termine lui-même en code `0`.

Ces essais portent sur un très petit volume simulé, pas sur une collecte SNCF continue. Les durées affichées par les cycles excluent le démarrage de Spark et ne mesurent pas la latence Kafka/source → PostgreSQL.

Dernière suite Python complète confirmée : 118 tests réussis. Après ajout des six tests de supervision commune, 25 tests ciblés ont réussi ; la nouvelle suite complète finale et la CI du commit retenu restent à confirmer.

## Utilisation

Prérequis : Docker Desktop, JDK 17, Maven, Python 3.12 ; Tableau Desktop et son pilote PostgreSQL pour la BI réelle.

Pour une installation complète, suivre le [runbook](docs/runbook.md). Sur une installation déjà préparée, démarrer les services nécessaires :

```bash
docker compose up -d kafka postgres
```

Kafka UI reste optionnel. Metabase n’est pas nécessaire au parcours Tableau ni aux essais actifs.

### Parcours réel à la demande

Avec topic, tables, environnement Python et références déjà prêts :

```bash
bash scripts/run_real_pipeline.sh \
  data/reference/real/collection-eYyLYD
```

Ce chemin est celui du lot historique : utiliser les références du lot traité. Il n’est pas recréé par Git. Cette commande traite les données déjà disponibles dans Kafka ; elle ne collecte ni ne publie de nouveaux événements. Actualiser ensuite les deux sources dans Tableau.

### Simulation à la demande

`run_demo.sh` produit un événement et lance le traitement ; `run_pipeline.sh` traite sans nouvel envoi. Conserver la configuration du parcours choisi et ne pas les lancer en parallèle d’un traitement visant les mêmes données/checkpoints.

### Simulation active supervisée

Lanceur : `scripts/run_active_simulation.py`. Avec `--with-gold`, il lance Bronze, Silver et le cycle Gold/PostgreSQL ; sans cette option, il conserve le mode Bronze/Silver seuls.

Préconditions : dossier de test déjà préparé avec Bronze Delta et les checkpoints attendus, références de simulation disponibles, topic existant et tables de `sncf_active_test` initialisées. Le script SQL dédié est `infra/postgres/003_active_test_gold_tables.sql` ; sa création échoue volontairement si le schéma existe déjà. Ne pas le supprimer pour contourner cette erreur.

Exemple du dossier local utilisé lors des essais :

```bash
.venv/bin/python scripts/run_active_simulation.py \
  data/validation/active-stream-ORGPHE \
  --interval 10 \
  --with-gold \
  --gold-interval 30 \
  --stop-timeout 30
```

Ce dossier et ses checkpoints ne sont pas fournis par Git. Cette commande n’est pas une procédure d’initialisation depuis un clone vierge. Utiliser son propre périmètre de test préparé ; ne pas lancer les jobs manuellement en doublon.

- `--interval` : intervalle de déclenchement Bronze/Silver, sans garantie de latence.
- `--gold-interval` : pause après un cycle ou un contrôle de version, pas une fréquence stricte de publication.
- Le lanceur ne produit aucun événement Kafka ; la publication Java reste séparée.
- Les logs sont dans le nouveau dossier affiché au démarrage : `bronze.log`, `silver.log`, `gold.log`.
- `Ctrl+C` demande l’arrêt de Gold, puis Silver et Bronze ; une escalade après dépassement du délai est signalée en erreur.
- `scripts/run_active_gold.py <dossier> --once` permet un cycle unique ; ne pas le lancer simultanément avec le Gold supervisé sur les mêmes données.

### Isolation des parcours

| Périmètre | Simulation à la demande | Réel à la demande | Test actif de simulation |
|---|---|---|---|
| Topic | `sncf.trip_updates.raw` | `sncf.trip_updates.real.raw` | `sncf.trip_updates.raw` — partagé avec la simulation |
| Delta | `data/lakehouse` | `data/lakehouse_real` | `data/validation/active-stream-<id>/lakehouse` |
| Checkpoints | `data/checkpoints` | `data/checkpoints_real` | `data/validation/active-stream-<id>/checkpoints` |
| Schéma PostgreSQL | `public` | `sncf_real` | `sncf_active_test` |

Le topic de simulation est partagé : un événement envoyé pendant le test actif pourra aussi être lu ultérieurement par le parcours de simulation habituel. Les données réelles restent distinctes. Le publisher vérifie les couples lakehouse/schéma autorisés, y compris le périmètre de test explicite.

## Fiabilité et limites

- Les contrôles Silver bloquent avant Gold : doublons, champs requis retenus, conversion secondes/minutes et complétude de `route_id`, `stop_id`, `line_name`, `stop_name`. La complétude ne garantit pas l’exactitude sémantique des noms GTFS ; une anomalie d’enrichissement n’est pas automatiquement déplacée vers les rejets.
- Le cycle Gold contrôle et agrège une même version Silver avec `versionAsOf`, puis recalcule les cumuls de cet instantané. Les deux écritures Delta et PostgreSQL ne constituent pas une transaction globale ; les échecs partiels restent à tester.
- La version publiée est mémorisée uniquement pendant l’exécution : Silver inchangée est ignorée, mais le premier cycle est rejoué après redémarrage. Les erreurs arrêtent le lanceur ; les retries/backoffs ne sont pas encore implémentés.
- La supervision détecte les sorties de processus, pas encore une requête bloquée, une source stagnante ou une fraîcheur insuffisante. Un arrêt normal ne garantit pas le vidage complet du pipeline.
- Les checkpoints et upserts couvrent les scénarios testés, sans garantie universelle d’exactly-once. La déduplication Silver est bornée par watermark.
- Le publisher reste limité à 10 000 agrégats par table. Les lignes disparues de Gold ne sont pas automatiquement supprimées de PostgreSQL.
- Le coût d’un recalcul complet n’a été observé que sur quelques événements. Le Mac de test dispose de 8 Go de RAM ; une pression mémoire jaune et un recours important au swap ont été observés avec les trois jobs et d’autres applications ouvertes. Aucun benchmark de charge soutenue n’est revendiqué.
- Les dashboards Tableau réels n’ont pas été basculés sur le schéma de test. Metabase reste historique et présent dans Compose. Les alertes, la DLQ Kafka et Airflow ne sont pas implémentés.
- Ne pas versionner `.env`, secrets, données Delta, checkpoints, logs ou exports volumineux. Les dossiers locaux `active-stream-*`, `trigger-regression-*` et les verrous des lanceurs doivent être ignorés ; conserver les données nécessaires à la reprise sur le disque.

## Documentation

- [Contrat de données](docs/data-contract.md) : format, mapping réel et couverture des contrôles.
- [Runbook](docs/runbook.md) : installation et parcours à la demande ; les procédures actives restent à y consolider.
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
- [x] Contrôles Silver bloquants, dont complétude de l’enrichissement et 14 cas ciblés testés.
- [x] Agrégats Gold et publication PostgreSQL par upsert avec garde-fou de périmètre.
- [x] Scripts de démonstration, simulation et traitement réel à la demande.
- [x] Validation des 110 observations réelles jusqu’à PostgreSQL et Tableau.
- [x] Deux dashboards Tableau, sources distinctes, moyennes pondérées et filtre testé.
- [x] Actualisation manuelle Tableau après nouvelle publication : passage de 10 à 110 observations.
- [x] Captures d’état paramétrables et reprise normale à la demande vérifiée en simulation et en réel.
- [x] Tests Java/Python et CI GitHub Actions existants ; résultats locaux distingués du statut CI.
- [x] Documentation du parcours à la demande révisée et simulation vérifiée depuis un clone isolé.
- [x] Configuration de déclenchement explicite, avec mode à la demande conservé par défaut.
- [x] Bronze/Silver actifs et reprise avec checkpoints testés sur un périmètre de simulation dédié.
- [x] Cycle Gold réutilisant une session Spark, contrôles et cumuls sur une version Silver figée.
- [x] Publication dans `sncf_active_test`, protections de chemins testées et republication à contenu identique.
- [x] Lanceur Gold périodique, version inchangée ignorée et arrêt coopératif corrigé/testé.
- [x] Supervision commune des trois jobs, logs séparés, verrous et scénarios d’arrêt vérifiés.
- [x] Propagation automatique d’une nouvelle observation simulée jusqu’à PostgreSQL : passage de 3 à 4.

### À faire — V1 quasi-temps réel

- [ ] Définir le volume et la cadence de collecte réelle après examen de la source, de ses contraintes et des ressources locales.
- [ ] Automatiser la collecte des instantanés SNCF, d’abord sans publication Kafka, avec gestion des erreurs et références compatibles.
- [ ] Reconnaître les instantanés inchangés et éviter leur republication ; tester la reprise d’une publication partielle.
- [ ] Adapter et valider le mode actif sur le parcours réel sans casser le mode à la demande ni effacer l’historique.
- [ ] Ajouter et tester les retries/backoffs et la récupération après échec partiel du cycle Gold/PostgreSQL.
- [ ] Mesurer fraîcheur source → PostgreSQL et délai collecteur → PostgreSQL ; détecter source stagnante et absence de progression.
- [ ] Tester les checkpoints réels, les interruptions en cours de traitement et les limites d’arrêt/vidage du pipeline actif.
- [ ] Mesurer le coût sur une session plus longue et un volume accru ; adapter les ressources ou la stratégie de recalcul si nécessaire.
- [ ] Démontrer plusieurs instantanés SNCF récents traités sans commande intermédiaire, puis vérifier Tableau après actualisation manuelle.
- [ ] Consolider les procédures actives, les limites et les preuves dans le runbook, le contrat et l’architecture.
- [ ] Revalider la suite complète après la dernière extension du superviseur et vérifier la CI du commit retenu.
- [ ] Vérifier le parcours réel et l’initialisation du mode actif depuis un clone isolé ; archiver les preuves sans secrets.
- [ ] Confiner les ports à l’interface locale et créer un compte BI en lecture seule.
- [ ] Vérifier le classeur Tableau, sa réouverture et l’absence de secrets avant versionnement.
- [ ] Actualiser les captures du README, vérifier le guide Tableau et enregistrer une démonstration propre.
- [ ] Retirer Metabase et ses références inutiles sans supprimer les volumes métier.
- [ ] Compléter documentation et qualité de code ; acter les contrôles du contrat, alertes et DLQ retenus pour la clôture.
- [ ] Ajouter les analyses temporelles seulement après validation d’un historique suffisant, sans en faire à elles seules une preuve de streaming.

### À faire — V2 et évolutions

- [ ] Ajouter Airflow pour planifier les références GTFS, les contrôles et les recalculs/publications batch.
- [ ] Documenter une éventuelle trajectoire Azure, optionnelle et non bloquante.
