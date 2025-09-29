package Main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DBConfig {
    private final String dbpath;

    public DBConfig(String dbpath) {
        this.dbpath = dbpath;
    }

    public String getDbPath() {
        return dbpath;
    }

    public static DBConfig loadFromFile(String filename) {
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            StringBuilder json = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                json.append(line.trim());
            }

            String content = json.toString();
            // ex: {"dbpath": "data"}
            content = content.replace("{", "")
                    .replace("}", "")
                    .replace("\"", "");
            String[] parts = content.split(":");
            if (parts.length == 2 && parts[0].trim().equals("dbpath")) {
                return new DBConfig(parts[1].trim());
            }

        } catch (IOException e) {
            System.out.println("Erreur lecture config: " + e.getMessage());
        }
        return new DBConfig("data");
    }
}
