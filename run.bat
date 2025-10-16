@echo off
REM -----------------------------
REM Script pour compiler et exécuter un projet Java
REM -----------------------------

REM Nettoyer l'ancien dossier 'bin' s'il existe
if exist bin rmdir /s /q bin

REM Créer un nouveau dossier 'bin'
mkdir bin

REM Compiler tous les fichiers Java
javac -d bin src\*.java src\config\*.java src\commandes\*.java

REM Exécuter le programme Java
java -cp bin Main

REM Pause pour garder la fenêtre ouverte
pause
