# azure-databricks-transport-public


## Description des Données

L'ensemble de données utilisé dans ce script est destiné à la gestion des données de transports publics. Il comprend des informations sur les départs, les arrivées, les retards, les passagers, les itinéraires, et plus encore, pour plusieurs mois.

## Transformations

Les transformations suivantes sont appliquées aux données brutes :

1. **Conversion des Horaires** : Les colonnes "ArrivalTime" et "DepartureTime" sont converties au format "HH:mm" pour représenter l'heure d'arrivée et de départ respectivement.

2. **Ajout de Colonnes de Date** : Les colonnes "Year", "Month", "Day" et "DayOfWeek" sont ajoutées pour décomposer les dates et faciliter les analyses basées sur le temps.

3. **Conversion en Horodatage** : Les colonnes "DepartureTime" et "ArrivalTime" sont converties en horodatages pour permettre des calculs de durée de trajet précis.

4. **Calcul de la Durée de Trajet** : La colonne "TripDurationMinutes" est calculée en minutes en fonction des horodatages de départ et d'arrivée. Une gestion spéciale est effectuée pour les trajets qui passent minuit.

5. **Catégorisation des Retards** : La colonne "DelayCategory" est ajoutée pour catégoriser les retards en "Pas de Retard," "Retard Court," "Retard Moyen," "Long Retard," ou "Inconnu."

6. **Analyse des Itinéraires** : Une analyse des itinéraires est effectuée pour calculer la moyenne des retards, le nombre moyen de passagers et le nombre total de trajets par itinéraire.

7. **Identification des Heures de Pointe** : La colonne "HeureDePointe" est ajoutée pour identifier les heures de pointe en fonction du nombre de passagers.

## Lignage des Données

Les données sont générées à partir d'un script Python qui simule les données de transport public. Les données générées sont stockées dans le stockage Data Lake Azure à l'emplacement spécifié dans "raw." Elles représentent des informations sur les départs, les arrivées, les retards, les passagers, les itinéraires, etc., pour plusieurs mois. Les sources simulées comprennent différents modes de transport public, tels que les bus, les trains, les tramways et les métros. Ces données sont ensuite traitées à l'aide d'Azure Databricks pour effectuer diverses transformations et analyses.

## Directives d'Utilisation

Les données traitées peuvent être utilisées pour divers cas d'utilisation, notamment :

- Analyse des horaires de transport public.
- Suivi des retards et de la ponctualité.
- Analyse de l'utilisation des itinéraires.
- Identification des heures de pointe.
- Études sur l'impact de la météo sur les transports publics.

Il est important de prendre en compte les catégories de retard pour une meilleure compréhension des performances du système de transport. Il est également important de noter que les données sont générées synthétiquement à des fins de démonstration et de test, ce qui peut entraîner des variations dans les données simulées.

