@echo off
REM Nettoyer ancien bin si existe
if exist bin rmdir /s /q bin
mkdir bin

REM
javac -d bin src\*.java src\config\*.java src\commandes\*.java

REM
java -cp bin Main

pause
