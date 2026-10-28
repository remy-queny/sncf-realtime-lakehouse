# SNCF Real-Time Lakehouse

Projet local de data engineering pour normaliser des observations SNCF GTFS-RT, les traiter dans un lakehouse Delta et restituer des indicateurs de retard dans Tableau.

> État au 28 octobre 2026 : parcours à la demande validé sur 110 observations réelles ; mode actif supervisé démontré en simulation jusqu’à PostgreSQL sur 4 observations. Une publication réelle supplémentaire de 100 observations a été acquittée par Kafka, puis sa non-republication a été vérifiée. Ce nouveau lot n’a pas encore été traité par Spark ni publié dans PostgreSQL/Tableau. La suite est organisée en six jalons de clôture ; le parcours réel actif complet reste à valider. Airflow reste prévu en V2.

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

### Publication réelle contrôlée — 28 octobre 2026

Cette validation porte sur la publication Kafka et son registre local, pas encore sur le traitement analytique du nouveau lot.

| Contrôle | Résultat observé |
|---|---|
| Périmètre sélectionné | 400 observations réparties en 4 lots de 100 |
| Registre après le premier envoi | 1 lot `published`, 3 lots `pending`, aucun `publishing` ni `uncertain` |
| Lot effectivement publié | 100 observations vers `sncf.trip_updates.real.raw` |
| Reçu de publication | 100 accusés de réception, partition `0`, offsets `110` à `209` |
| Offset de fin après publication | `210` |
| Relance du même lot déjà publié | `published_now: false`, statut conservé à `published` |
| Contrôle broker avant/après cette relance | Offset de fin inchangé à `210`, comparaison des captures sans différence |

Le test valide l’absence de republication du lot déjà terminé dans ce scénario. Il ne démontre ni une garantie générale d’exactly-once ni la récupération après une publication partielle ou un résultat incertain. Les 300 observations des trois autres lots restent en attente ; leur publication n’est pas un prérequis au contrôle du premier lot de bout en bout.

Une capture analytique a été réalisée avant de traiter le nouveau lot :

| Élément capturé | État avant traitement |
|---|---:|
| Silver `trip_delays` | 110 observations |
| Silver `rejected_events` | 0 rejet |
| `sncf_real.gold_delay_by_line_15min` | 7 agrégats |
| `sncf_real.gold_station_delay_daily` | 84 agrégats |

La validation analytique de bout en bout reste limitée aux 110 observations historiques. Les 100 observations supplémentaires acquittées par Kafka ne portent pas encore ce total à 210 dans Silver, PostgreSQL ou Tableau. Les nombres d’agrégats ne sont pas des nombres d’observations.

Preuves locales de cette session, sous `data/source_checks/active-real-ldCSXV/` : registre et reçus dans `publication/`, contrôles d’offset dans `publication/check-csPBuD/`, capture analytique dans `serving-check-FP8X7F/before.json`. Ces dossiers ne sont pas fournis par Git et ne constituent pas une procédure d’initialisation depuis un clone.

### Simulation — mode actif supervisé

- Bronze/Silver actifs et propagation d’un nouvel événement jusqu’à PostgreSQL sans recalcul manuel : passage de 3 à 4 observations.
- État final du test : 4 agrégats et 4 observations dans chacune des deux tables de `sncf_active_test` ; ne pas additionner leurs volumes.
- Reprise Bronze/Silver avec les mêmes checkpoints : aucune nouvelle ligne écrite sans nouvel envoi, puis traitement d’un événement supplémentaire.
- Inspection Silver intermédiaire : 3 observations, 0 rejet et quatre contrôles qualité réussis ; contrôles également réussis dans les cycles Gold suivants.
- Republication à Gold inchangé : contenu complet des deux tables PostgreSQL identique avant/après.
- Supervision commune : démarrage des trois jobs, arrêt par un seul `Ctrl+C`, sortie inattendue de Silver testée sous supervision Bronze/Silver, puis sortie inattendue de Gold testée sous supervision des trois jobs.
- Arrêt normal du superviseur en code `0` ; perte d’un job signalée en code `1`, même si ce job termine lui-même en code `0`.

Ces essais portent sur un très petit volume simulé, pas sur une collecte SNCF continue. Les durées affichées par les cycles excluent le démarrage de Spark et ne mesurent pas la latence Kafka/source → PostgreSQL.

Historique des résultats de tests documentés : 118 tests Python réussis sur une suite complète, puis 25 tests ciblés après ajout des six tests de supervision commune. Ces compteurs ne constituent pas un nouveau relevé global au 28 octobre. Les contrôles Kafka ci-dessus sont des validations d’intégration distinctes et ne permettent pas de déduire un nouveau total de tests. La suite complète finale et la CI du commit retenu restent à confirmer.

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
STREAMING_MODE=available_now bash scripts/run_real_pipeline.sh \
  data/reference/real/collection-eYyLYD
```

Ce chemin est celui du lot historique : utiliser les références compatibles avec le lot traité, sans reprendre cet exemple aveuglément pour la collecte du 28 octobre. Il n’est pas recréé par Git. Le mode `available_now` est explicite pour conserver une exécution à la demande. Cette commande traite les données déjà disponibles dans Kafka ; elle ne collecte ni ne publie de nouveaux événements. Conserver les checkpoints existants et ne pas lancer un second traitement sur le même périmètre. Actualiser les deux sources dans Tableau après validation du traitement et de la publication PostgreSQL.

### Publication réelle et test de non-republication

Le lanceur `scripts/publish_real_publication.py` publie un lot explicitement sélectionné avec `--batch-id` et `--execute`. La publication Kafka est distincte du traitement Spark/Gold/PostgreSQL.

Exemple utilisé pour tester la relance du lot déjà `published` de cette session :

```bash
.venv/bin/python scripts/publish_real_publication.py \
  data/source_checks/active-real-ldCSXV \
  --batch-id 45b99e449061724d8ff028ba8451cfd670741eb65d20c2e4c40988a429b2f5d1 \
  --execute
```

Dans l’état validé de ce registre, la relance retourne `published_now: false`, `status: published` et `event_count: 100`. L’offset de fin Kafka est resté à `210` avant et après la relance. La présence d’un chemin de reçu ou de log dans la réponse ne signifie pas qu’un nouvel envoi a eu lieu.

Cet exemple documente un test de non-republication, pas une préparation ou une première publication depuis un clone. Ne pas recréer le registre pour forcer un envoi, supprimer les reçus ou appliquer cette relance aveuglément à un lot en échec, `publishing` ou `uncertain`. Les trois lots `pending` ne font pas partie de ce test.

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
- Pour le cycle Gold/PostgreSQL, la version Silver publiée est mémorisée uniquement pendant l’exécution : Silver inchangée est ignorée, mais le premier cycle est rejoué après redémarrage. Ce mécanisme est distinct du registre local de publication Kafka. Les erreurs arrêtent le lanceur ; les retries/backoffs du cycle Gold restent à implémenter et tester.
- La supervision détecte les sorties de processus, pas encore une requête bloquée, une source stagnante ou une fraîcheur insuffisante. Un arrêt normal ne garantit pas le vidage complet du pipeline.
- Pour la publication Kafka réelle, le reçu de 100 accusés et le refus de republier un lot déjà `published` ont été vérifiés. La reprise après envoi partiel et les situations `publishing`/`uncertain` restent à valider ; un échec ne justifie pas de remettre aveuglément le lot à `pending` ou d’effacer son registre.
- Les checkpoints et upserts couvrent les scénarios testés, sans garantie universelle d’exactly-once. La déduplication Silver est bornée par watermark.
- Le publisher reste limité à 10 000 agrégats par table. Les lignes disparues de Gold ne sont pas automatiquement supprimées de PostgreSQL.
- Le coût d’un recalcul complet n’a été observé que sur quelques événements. Le Mac de test dispose de 8 Go de RAM ; une pression mémoire jaune et un recours important au swap ont été observés avec les trois jobs et d’autres applications ouvertes. Aucun benchmark de charge soutenue n’est revendiqué.
- Les dashboards Tableau réels n’ont pas été basculés sur le schéma de test. Metabase reste historique et présent dans Compose. Les alertes, la DLQ Kafka et Airflow ne sont pas implémentés.
- Ne pas versionner `.env`, secrets, données Delta, checkpoints, logs, reçus de publication ou exports volumineux. Les données et preuves locales de `data/source_checks/` ne sont pas nécessaires au clone du code. Les dossiers locaux `active-stream-*`, `trigger-regression-*` et les verrous des lanceurs doivent être ignorés ; conserver les données nécessaires à la reprise sur le disque.

## Documentation

- [Contrat de données](docs/data-contract.md) : format, mapping réel et couverture des contrôles.
- [Runbook](docs/runbook.md) : installation et parcours à la demande ; les procédures actives restent à y consolider.
- [Guide Tableau](bi/tableau/README.md) : connexion, calculs, filtres et réouverture.
- [Validation historique de la source réelle](docs/real-source.md) : premier instantané et résultats d’origine.

## Avancement

### Socle déjà validé

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
- [x] Publication réelle d’un lot de 100 observations avec 100 accusés Kafka, offsets `110` à `209`.
- [x] Refus de republier le même lot déjà terminé : `published_now: false` et offset de fin inchangé à `210`.
- [x] Capture analytique avant traitement du nouveau lot : 110 observations Silver, 0 rejet et 7/84 agrégats PostgreSQL.

### Clôture V1 — six jalons bornés

Les composants du socle ci-dessus sont acquis : ils ne sont pas à reconstruire. Cette liste organise le reste à faire ; elle distingue les tâches d’intégration, les validations et les finitions. Une vérification peut confirmer un point déjà réalisé sans nécessiter de nouveau développement. Les sous-points ne sont pas de nouveaux jalons.

Ordre de progression : nouveau lot de bout en bout → session réelle automatique → mesures et reprise → clôture technique et présentation. Aucun pourcentage d’avancement n’est déduit du nombre de cases.

#### Jalon 1 — Valider le nouveau lot réel jusqu’à PostgreSQL

- [x] Publier 100 observations dans Kafka et vérifier les 100 accusés de réception.
- [x] Vérifier la non-republication du lot déjà terminé.
- [x] Capturer l’état analytique avant traitement : 110 observations, 0 rejet, 7/84 agrégats.
- [ ] Identifier le référentiel GTFS utilisé pour normaliser le nouveau lot.
- [ ] Exécuter le parcours réel en `available_now`, avec les checkpoints existants.
- [ ] Contrôler les nouvelles observations Silver : présence, déduplication, enrichissement et rejets éventuels.
- [ ] Vérifier la conservation des 110 observations historiques et la publication des agrégats dans PostgreSQL, puis comparer les captures avant/après.

Critère de fin : le nouveau lot est validé jusqu’à PostgreSQL, sans perte d’historique. Ne pas confondre le nombre d’agrégats avec le nombre d’observations.

#### Jalon 2 — Raccorder et valider la session réelle automatique

Réutiliser le collecteur, la normalisation, le registre et les composants actifs existants ; ne pas reconstruire le mode simulé déjà validé.

- [ ] Finaliser la cadence et le volume de collecte selon les contraintes de la source et du Mac.
- [ ] Raccorder et valider collecte → normalisation → publication Kafka sans commande manuelle entre les instantanés.
- [ ] Vérifier la gestion des instantanés inchangés et l’utilisation du registre de publication dans cet enchaînement.
- [ ] Adapter au périmètre réel la supervision Bronze/Silver et le cycle Gold/PostgreSQL, avec les protections de chemins et de schémas.
- [ ] Préserver le mode à la demande et l’isolation entre simulation et réel.
- [ ] Démontrer le traitement automatique de plusieurs instantanés récents jusqu’à PostgreSQL.

Critère de fin : après lancement, la session réelle progresse sans déclenchement manuel de chaque étape. Un service permanent 24 h/24 et un rafraîchissement automatique de Tableau ne sont pas exigés.

#### Jalon 3 — Mesurer la fraîcheur et suivre la progression

- [ ] Mesurer séparément la fraîcheur source → PostgreSQL et le délai collecteur → PostgreSQL.
- [ ] Conserver les horodatages et informations de progression nécessaires : dernière collecte, publication Kafka, traitement et publication analytique.
- [ ] Distinguer une source inchangée d’un pipeline bloqué et définir des seuils simples pour signaler l’absence de progression.
- [ ] Observer une session plus longue et un volume supérieur aux petits essais : mémoire, swap, durée des cycles et retard éventuel.

Critère de fin : présenter des délais mesurés et les limites observées, sans confondre durée d’un cycle Gold et latence de bout en bout. Définir un objectif de fraîcheur adapté aux mesures, sans promettre un délai non démontré.

#### Jalon 4 — Valider la reprise réelle active et les pannes retenues

La reprise historique à la demande et les scénarios d’arrêt du superviseur simulé sont acquis. Les validations restantes concernent le parcours réel actif et les nouveaux composants.

- [ ] Tester l’arrêt et le redémarrage réels avec les mêmes checkpoints et le même registre ; contrôler doublons et conservation de l’historique.
- [ ] Tester une interruption de publication Kafka et documenter le diagnostic des états `publishing`/`uncertain`, sans renvoi aveugle.
- [ ] Tester un échec partiel du cycle Gold/PostgreSQL et sa récupération.
- [ ] Ajouter et tester les retries/backoffs prévus uniquement sur les opérations dont la relance est sûre ; ne pas assimiler un envoi Kafka incertain à un simple retry.
- [ ] Documenter les limites de l’arrêt et du vidage des données en attente.

Critère de fin : les scénarios retenus ont un comportement testé et une procédure de récupération explicite. Ne pas promettre un exactly-once universel ni une récupération automatique de toutes les pannes.

#### Jalon 5 — Valider la version finale, la reproductibilité et la sécurité

- [ ] Exécuter les suites complètes Java/Python et vérifier la CI du commit retenu.
- [ ] Confirmer l’absence de régression des parcours simulation et réel à la demande.
- [ ] Vérifier, depuis un clone isolé, l’installation et l’initialisation des parcours documentés sans dépendre des dossiers temporaires de développement.
- [ ] Vérifier et, si nécessaire, confiner les ports à l’interface locale.
- [ ] Créer et utiliser un compte PostgreSQL en lecture seule pour Tableau.
- [ ] Vérifier l’absence de secrets dans Git, les logs partagés et le classeur Tableau.
- [ ] Retirer Metabase et ses références inutiles sans supprimer les volumes métier.

Critère de fin : une version testée et reproductible, avec les services locaux et identifiants correctement protégés. Un point à vérifier n’est pas automatiquement un point à réimplémenter.

#### Jalon 6 — Finaliser Tableau, la documentation et la démonstration

Les deux dashboards existent déjà : les vérifier et les présenter, sans les reconstruire.

- [ ] Après publication PostgreSQL, actualiser les deux sources Tableau et vérifier périodes, filtres et indicateurs sur le nouveau périmètre par comparaison avec PostgreSQL.
- [ ] Vérifier la sauvegarde et la réouverture du classeur sans secrets.
- [ ] Confirmer ou actualiser les captures propres des dashboards.
- [ ] Enregistrer une courte démonstration du parcours réel automatique, puis de l’actualisation manuelle Tableau.
- [ ] Consolider README, runbook, architecture, contrat de données et contexte : résultats, limites, commandes de démarrage, d’arrêt et de reprise.
- [ ] Acter explicitement le périmètre de clôture pour les contrôles additionnels, alertes et DLQ : fonctionnalité retenue ou évolution reportée, sans ajout implicite de nouvelles exigences.

Critère de fin : une autre personne peut comprendre le projet, reproduire le parcours documenté et distinguer les résultats démontrés des fonctionnalités envisagées.

### Hors du chemin de clôture — V2 et évolutions

Ces éléments ne doivent pas allonger automatiquement les six jalons :

- [ ] Airflow en V2 pour planifier références GTFS, contrôles, recalculs et publications batch.
- [ ] Trajectoire Azure optionnelle, sans dépendance cloud pour terminer la V1 locale.
- [ ] Analyses temporelles, heatmaps, distributions et percentiles après validation d’un historique et d’une granularité adaptés.
- [ ] Topics `validated`, alertes de service et DLQ si retenus explicitement comme extensions.
- [ ] Haute disponibilité, fonctionnement permanent et tests de charge poussés : hors de la démonstration locale visée.
