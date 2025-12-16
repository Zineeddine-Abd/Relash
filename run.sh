# Nettoyer l'ancien dossier bin si existe
rm -rf bin
mkdir bin

# Compilation de tous les fichiers .java trouvés dans src
javac -d bin $(find src -name "*.java")

# Lancement du point d'entrée correct : Package Main, Classe SGBD
java -cp bin Main.SGBD
