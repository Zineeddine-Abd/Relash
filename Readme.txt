========================================================================
                      CONFIGURATION BIG DATA & NOTES
========================================================================

1. CONFIGURATION "BIG DATA"
------------------------------------------------------------------------
Les valeurs suivantes ont été spécifiquement choisies dans le fichier 
config/dbconfig.json pour garantir la performance et la stabilité du 
SGBD lors du traitement de gros volumes de données (Scénarios Big Data) :

{
  "dbpath": "data",
  "pagesize": 32768,            
  "dm_maxfilecount": 20,
  "dm_maxpagesperfile": 1000000,
  "bm_buffercount": 5000,       
  "bm_policy": "LRU"
}

DÉTAIL DES PARAMÈTRES :

- dbpath ("data") : 
  Chemin du dossier où le SGBD stocke physiquement toutes les données 
  (fichiers .bin, sauvegardes, métadonnées, etc.) sur le disque.

- pagesize (32768) : 
  Taille fixe d’une page en octets (32 Ko).

- dm_maxfilecount (20) : 
  Nombre maximum de fichiers de données (ex : Data0.bin, Data1.bin) 
  que le DiskManager est autorisé à créer dans le dossier BinData.

- dm_maxpagesperfile (1000000) : 
  Nombre maximal de pages qu’un seul fichier .bin peut contenir avant 
  que le système ne crée automatiquement un nouveau fichier.

- bm_buffercount (5000) : 
  Capacité de la mémoire cache (RAM). Correspond au nombre de pages 
  que le BufferManager peut conserver simultanément en mémoire 
  (nombre de frames).

- bm_policy ("LRU") : 
  Politique de remplacement (Least Recently Used). Lorsque la mémoire 
  est pleine, la page utilisée le moins récemment est sélectionnée 
  pour être remplacée.

JUSTIFICATION DES CHOIX :
- pagesize (32768) : Une grande taille de page (32 Ko) réduit le nombre 
  d'appels système (I/O) lors des lectures séquentielles massives 
  (ex: SELECT sur des milliers de lignes).
  
- bm_buffercount (5000) : Un grand nombre de buffers permet de garder 
  une partie significative de la base de données en mémoire (RAM), 
  minimisant les accès disques lents lors des opérations lourdes comme 
  APPEND ou les jointures futures.

- dm_maxfilecount & dm_maxpagesperfile : Ces limites élevées assurent 
  une capacité de stockage virtuel quasi-illimitée pour les besoins 
  du projet, évitant les erreurs "Disque Plein" lors d'insertions massives.

2. EMPLACEMENT DES FICHIERS CSV
------------------------------------------------------------------------
Pour utiliser la commande d'importation :
   APPEND INTO <Table> ALLRECORDS (<Fichier.csv>)

IMPORTANT : Le fichier .csv doit IMPÉRATIVEMENT être placé à la RACINE 
du projet (au même niveau que le dossier 'src', 'bin' et les scripts 
'run.bat'/'run.sh').

Le programme ne cherche pas les fichiers dans des sous-dossiers.

3. COMPILATION ET EXÉCUTION
------------------------------------------------------------------------
Des scripts sont fournis pour simplifier la compilation et l'exécution 
du projet.

SOUS WINDOWS :
Double-cliquez sur le fichier 'run.bat' ou lancez la commande suivante 
dans un terminal (CMD ou PowerShell) :
   .\run.bat

SOUS LINUX / MACOS :
Il faut d'abord donner les droits d'exécution au script, puis le lancer :
   chmod +x run.sh
   ./run.sh