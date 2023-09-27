# Projet Azure Databricks pour la Gestion des Données de Transport Public

## Description des Données

Ce projet est conçu pour gérer les données relatives aux transports publics, en réponse à l'urbanisation croissante et à la demande de mobilité. L'ensemble de données comprend des informations sur les départs, les arrivées, les retards, les passagers, les itinéraires, et bien plus encore, couvrant une période de plusieurs mois.

## Automatisation du Pipeline

Le pipeline d'automatisation se compose de trois étapes essentielles :

### Étape 1 : Génération des Données
Dans cette première étape, un script est exécuté pour générer des données synthétiques qui simulent les opérations de transport public. Ces données générées sont ensuite stockées dans le répertoire "raw" de Azure Data Lake Storage.

### Étape 2 : Traitement des Données
La deuxième étape implique l'exécution d'un script pour traiter les données brutes générées précédemment. Les données sont lues à l'aide d'Azure Databricks avec PySpark, puis elles subissent une série de transformations, notamment la conversion des horaires, le calcul de la durée des trajets, la catégorisation des retards, l'analyse des itinéraires, et bien plus encore. Les données transformées sont sauvegardées dans le répertoire "processed" de Azure Data Lake Storage.

### Étape 3 : Archivage et Suppression des Données Brutes
La dernière étape du pipeline consiste à exécuter un notebook script pour archiver les données brutes, c'est-à-dire les déplacer vers un répertoire ou un conteneur d'archive, afin de libérer de l'espace de stockage. Une fois les données brutes archivées, elles sont supprimées si elles ne sont plus nécessaires, conformément aux politiques de conservation des données.

## Transformations

Les données brutes sont soumises à plusieurs transformations pour en faciliter l'analyse et améliorer la prise de décision. Les principales transformations incluent :

1. **Conversion des Horaires** : Les horaires d'arrivée et de départ sont convertis au format "HH:mm" pour une meilleure lisibilité.

2. **Ajout de Colonnes de Date** : Des colonnes "Year", "Month", "Day" et "DayOfWeek" sont ajoutées pour permettre des analyses basées sur le temps.

3. **Conversion en Horodatage** : Les horaires de départ et d'arrivée sont convertis en horodatages pour des calculs de durée de trajet précis.

4. **Calcul de la Durée de Trajet** : La durée de chaque trajet est calculée en minutes en fonction des horodatages de départ et d'arrivée, y compris la gestion des trajets passant minuit.

5. **Catégorisation des Retards** : Une colonne "DelayCategory" est ajoutée pour classer les retards en "Pas de Retard", "Retard Court" (1-10 minutes), "Retard Moyen" (11-20 minutes), "Long Retard" (>20 minutes) ou "Inconnu".

6. **Analyse des Itinéraires** : Une analyse des itinéraires est réalisée pour calculer le retard moyen, le nombre moyen de passagers et le nombre total de voyages pour chaque itinéraire.

7. **Identification des Heures de Pointe** : Une colonne "HeureDePointe" est ajoutée pour identifier les heures de pointe en fonction du nombre de passagers.

## Lignage des Données

Les données utilisées dans ce projet sont générées à partir d'un script Python qui simule les informations de transport public. Ces données simulées sont stockées dans le stockage Azure Data Lake à l'emplacement spécifié dans "/public_transport_data/raw/". Elles représentent divers modes de transport public, tels que les bus, les trains, les tramways et les métros, sur une période de plusieurs mois. Ensuite, ces données sont traitées à l'aide d'Azure Databricks pour appliquer les transformations et les analyses nécessaires.

## Directives d'Utilisation

Les données transformées peuvent être utilisées pour plusieurs cas d'utilisation, notamment :

- Analyse des horaires de transport public pour améliorer la planification.
- Suivi des retards et de la ponctualité pour prendre des mesures correctives.
- Analyse de l'utilisation des itinéraires pour optimiser les ressources.
- Identification des heures de pointe pour ajuster les services.
- Études sur l'impact de la météo sur les transports publics.

Il est essentiel de prendre en compte les catégories de retards pour une meilleure compréhension de la performance globale du système de transport. Il convient de noter que les données sont générées de manière synthétique à des fins de démonstration et de test, ce qui peut entraîner des variations dans les données simulées.

<img width="1000" alt="automatisation" src="https://github.com/omardbaa/azure-databricks-transport-public/assets/105659023/4c838149-549e-4028-9369-a9c04517e1c4">


