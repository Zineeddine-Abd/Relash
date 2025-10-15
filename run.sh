# Nettoyer l'ancien dossier bin si existe
rm -rf bin
mkdir bin

javac -d bin $(find src -name "*.java")

java -cp bin Main
