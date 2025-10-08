package Main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class DBConfig {
    private final String dbpath;
    private final int pagesize;
    private final int dm_maxfilecount;
    private final int dm_maxpagesperfile;
    private int bm_buffercount;
    private String bm_policy;

    public DBConfig(String dbpath, int pagesize, int dm_maxfilecount, int dm_maxpagesperfile) {
        this.dbpath = dbpath;
        this.dm_maxpagesperfile = dm_maxpagesperfile;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
        this.bm_buffercount = bm_buffercount;
        this.bm_policy = bm_policy;
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

    public int getDmMaxPagesPerFile() {
        return dm_maxpagesperfile;
    }

    public int getBm_buffercount() {
        return bm_buffercount;
    }

    public void setBm_buffercount(int bm_buffercount) {
        this.bm_buffercount = bm_buffercount;
    }

    public String getBm_policy() {
        return bm_policy;
    }

    public void setBm_policy(String bm_policy) {
        this.bm_policy = bm_policy;
    }

    public static DBConfig loadFromFile(String filename) {
        String dbpath = null;
        int pagesize = 0;
        int dm_maxfilecount = 0;
        int dm_maxpagesperfile = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            StringBuilder json = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                json.append(line.trim());
            }

            String content = json.toString();
            content = content.replace("{", "")
                    .replace("}", "")
                    .replace("\"", "")
                    .replace(" ", "");

            String[] pairs = content.split(",");

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
                        case "dm_maxpagesperfile":
                            dm_maxpagesperfile = Integer.parseInt(value);
                            break;
                    }
                }
            }
            return new DBConfig(dbpath, pagesize, dm_maxfilecount, dm_maxpagesperfile);
        } catch (IOException e) {
            System.out.println("Erreur lecture config: " + e.getMessage());
            return null;
        }

    }
}