@echo off
REM -----------------------------
REM Script pour compiler et exécuter le projet SGBD
REM -----------------------------

REM Nettoyer l'ancien dossier 'bin' s'il existe
if exist bin rmdir /s /q bin

REM Créer un nouveau dossier 'bin'
mkdir bin

REM Compiler tous les packages du projet
REM On liste ici tous les sous-dossiers présents dans votre src
javac -d bin src\Main\.java src\BufferManager\.java src\DBManager\.java src\DiskManager\.java src\FileManager\.java src\QueryManager\.java

REM Exécuter le programme Java
REM Le point d'entrée est la classe SGBD dans le package Main
java -cp bin Main.SGBD

REM Pause pour garder la fenêtre ouverte en cas d'erreur
pause