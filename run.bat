@echo off
REM Nettoyer ancien bin si existe
if exist bin rmdir /s /q bin
mkdir bin

javac -d bin src\*.java src\commandes\*.java src\Main\*.java

java -cp bin Main

pause