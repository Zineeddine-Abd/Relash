package Main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DBConfig {
    private final String dbpath;
    private final int pagesize;
    private final int dm_maxfilecount;

    public DBConfig(String dbpath, int pagesize, int dm_maxfilecount) {
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
    }

    public String getDbPath() {
        return dbpath;
    }

    public int getPageSize() {
        return pagesize;
    }

    public int getDmMaxFileCount() {
        return dm_maxfilecount;
    }

    public static DBConfig loadFromFile(String filename) {

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            StringBuilder json = new StringBuilder();
            String line;

            while ((line = br.readLine()) != null) {
                json.append(line.trim());
            }

            String content = json.toString();
            content = content.replace("{", "")
                    .replace("}", "")
                    .replace("\"", "")
                    .replace(" ", "");

            String[] pairs = content.split(",");
            String dbpath = "data";
            int pagesize = 4096;
            int dm_maxfilecount = 4;

            for (String pair : pairs) {
                String[] keyValue = pair.split(":");
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();

                    switch (key) {
                        case "dbpath":
                            dbpath = value;
                            break;
                        case "pagesize":
                            pagesize = Integer.parseInt(value);
                            break;
                        case "dm_maxfilecount":
                            dm_maxfilecount = Integer.parseInt(value);
                            break;
                    }
                }
            }

            return new DBConfig(dbpath, pagesize, dm_maxfilecount);

        } catch (IOException e) {
            System.out.println("Erreur lecture config: " + e.getMessage());
        }
        return new DBConfig("data", 4096, 4);
    }
}