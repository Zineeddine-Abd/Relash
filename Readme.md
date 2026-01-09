![Banner](./Relash_Github_Header.png)

# Relash - Mini SGBD - Projet BDD Avancées

Ce projet implémente un Système de Gestion de Base de Données (SGBD) relationnel simplifié en **Java**, partant de la gestion bas niveau (Disque/Buffer) jusqu'au moteur d'exécution de requêtes SQL.

## Fonctionnalités

* **Stockage de données** : Organisation en *Heap Files* avec gestion des pages via une *ByteMap*.
* **Gestion de la mémoire** : *Buffer Manager* avec politiques de remplacement **LRU** et **MRU**.
* **Types supportés** : `INT`, `FLOAT`, `CHAR(T)`, `VARCHAR(T)`.
* **Persistance** : Sauvegarde des données et du schéma des tables à l'arrêt (`EXIT`).
* **Import CSV** : Chargement en masse de données via la commande `APPEND`.

## Commandes Supportées

Le système supporte un sous-ensemble du langage SQL :

* **DDL (Définition)** :
    * `CREATE TABLE Nom (Col1:Type1, ...)`
    * `DROP TABLE Nom` / `DROP TABLES`
    * `DESCRIBE TABLE Nom` / `DESCRIBE TABLES`
* **DML (Manipulation)** :
    * `INSERT INTO Nom VALUES (v1, v2, ...)`
    * `APPEND INTO Nom ALLRECORDS (fichier.csv)`
    * `SELECT ... FROM ... WHERE ...` (Supporte les projections et filtres multiples)
    * `UPDATE Nom SET col=val WHERE ...`
    * `DELETE Nom WHERE ...`
* **Système** :
    * `EXIT` (Sauvegarde l'état et quitte proprement)

## Configuration
Les valeurs suivantes ont été choisies (par defaut) pour garantir de bonnes performances dans un **scénario Big Data**.

Le fichier de configuration se trouve dans `config/dbconfig.json`. Il permet de régler :
- *`dbpath` (`"data"`)* 
  Chemin du dossier où le SGBD stocke physiquement toutes les données  
  *(fichiers `.bin`, sauvegardes, métadonnées, etc.)* sur le disque.

- *`pagesize` (`32768`)*
  Taille fixe d’une page en octets.  

- *`dm_maxfilecount` (`20`)* 
  Nombre maximum de fichiers de données (ex : `Data0.bin`, `Data1.bin`) que le
  **DiskManager** est autorisé à créer dans le dossier `BinData`.

- *`dm_maxpagesperfile` (`1000000`)*  
  Nombre maximal de pages qu’un seul fichier `.bin` peut contenir avant que le
  système ne crée automatiquement un nouveau fichier.

- *`bm_buffercount` (`5000`)*  
  Capacité de la mémoire cache (RAM).  
  Correspond au nombre de pages que le **BufferManager** peut conserver
  simultanément en mémoire (nombre de frames).

- *`bm_policy` (`"LRU"`)* 
  Politique de remplacement (*Least Recently Used*).  
  Lorsque la mémoire est pleine, la page utilisée le moins récemment est
  sélectionnée pour être remplacée.
  
## Installation et Exécution

### Prérequis
* Java JDK installé.
* **Important** : Pour utiliser la commande `APPEND`, les fichiers `.csv` (ex: `S.csv`) doivent impérativement être placés à la **racine du projet** (au même niveau que les dossiers `src`, `bin` et les scripts `run`).

### Compiler et Lancer

Des scripts sont fournis pour simplifier la compilation et l'exécution.

Sous Windows : 
Double-cliquez sur run.bat ou lancez dans un terminal :
```bash
.\run.bat
```
Sous Linux / macOS : 
Donnez les droits d'exécution puis lancez le script :
```bash
chmod +x run.sh
```
```bash
./run.sh
```
