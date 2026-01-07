rm -rf bin
mkdir bin

javac -d bin -encoding UTF-8 src/Main/*.java src/BufferManager/*.java src/DBManager/*.java src/DiskManager/*.java src/FileManager/*.java src/QueryManager/*.java

java -cp bin Main.SGBD