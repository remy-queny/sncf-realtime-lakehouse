# Dashboard Metabase — Simulation

## Objectif

Visualiser le retard moyen par ligne et par arrêt à partir des agrégats
publiés dans PostgreSQL.

Les données et les noms sont simulés. Ils ne représentent pas des mesures
réelles de ponctualité SNCF.

## Connexion PostgreSQL

Metabase utilise la connexion PostgreSQL du projet :

- Hôte : `postgres`, sur le réseau Docker Compose.
- Port : `5432`.
- Base et utilisateur : valeurs configurées dans `.env`.
- Mot de passe : saisir localement, sans le versionner.

Le dashboard n'est pas provisionné automatiquement.
Les questions doivent être créées et enregistrées dans Metabase.

## Question 1 — Retard moyen par ligne

- Table : `gold_delay_by_line_15min`.
- Titre : `Retard moyen par ligne (simulation)`.
- Dans « Résumer », utiliser cette expression personnalisée :

```text
Sum([Average Delay Minutes] * [Event Count]) / Sum([Event Count])
```

- Nom de la mesure : `Retard moyen (minutes)`.
- Regroupement : `Line Name`.
- Visualisation : graphique en barres.
- Axe X : `Line Name`.
- Axe Y : `Retard moyen (minutes)`.

Sélectionner les champs depuis l'éditeur si leurs noms affichés diffèrent.
Enregistrer la question.

## Question 2 — Retard moyen par gare

- Table : `gold_station_delay_daily`.
- Titre : `Retard moyen par gare (simulation)`.
- Dans « Résumer », utiliser la même expression :

```text
Sum([Average Delay Minutes] * [Event Count]) / Sum([Event Count])
```

- Nom de la mesure : `Retard moyen (minutes)`.
- Regroupement : `Stop Name`.
- Visualisation : graphique en barres.
- Axe X : `Stop Name`.
- Axe Y : `Retard moyen (minutes)`.

Enregistrer la question.

## Assemblage du dashboard

1. Créer le dashboard `Suivi des retards ferroviaire (simulation)`.
2. Ajouter les deux questions enregistrées.
3. Disposer les graphiques côte à côte.
4. Enregistrer le dashboard.

La configuration décrite utilise toutes les dates disponibles.
Un filtre de période devra être documenté séparément s'il est ajouté.

## Calcul de la moyenne

Chaque ligne Gold peut représenter plusieurs événements.
La moyenne sur plusieurs lignes Gold doit donc être pondérée par
`event_count`.

Ne pas additionner les moyennes des fenêtres ou des jours.

Les moyennes Gold sont déjà arrondies à deux décimales :
leur recomposition peut être légèrement approximative sur certains
jeux de données.

## Résultats du jeu de validation

Pour le jeu de quatre événements validé le 8 octobre 2026 :

| Ligne / arrêt simulé | Événements | Retard moyen |
|---|---:|---:|
| J | 2 | 12 minutes |
| L | 1 | 10 minutes |
| H | 1 | 0 minute |

Ces valeurs sont un repère de validation, pas des valeurs fixes
pour les prochaines démonstrations.

Capture : `docs/screenshots/dashboard-simulation.png`.

## Actualisation

Le pipeline n'est pas exécuté en permanence.

- `bash scripts/run_demo.sh` produit un nouvel événement et lance
  le pipeline.
- `bash scripts/run_pipeline.sh` lance seulement les traitements.
- Actualiser ensuite les questions du dashboard.

L'actualisation du dashboard ne déclenche pas les jobs Spark.