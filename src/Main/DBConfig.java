package Main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DBConfig {
    private final String dbpath;
    private final int pagesize;
    private final int dm_maxfilecount;
    private final int dm_maxpagesperfile;
    private final int bm_buffercount;
    private final String bm_policy;

    public DBConfig(String dbpath, int pagesize, int dm_maxfilecount, int dm_maxpagesperfile, int bm_buffercount, String bm_policy) {
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
        this.dm_maxpagesperfile = dm_maxpagesperfile;
        this.bm_buffercount = bm_buffercount;
        this.bm_policy = bm_policy;
    }

    public String getDbPath() { return dbpath; }
    public int getPageSize() { return pagesize; }
    public int getDmMaxFileCount() { return dm_maxfilecount; }
    public int getDmMaxPagesPerFile() { return dm_maxpagesperfile; }
    public int getBm_buffercount() { return bm_buffercount; }
    public String getBm_policy() { return bm_policy; }

    public static DBConfig loadFromFile(String filename) {
        String dbpath = null;
        int pagesize = 0;
        int dm_maxfilecount = 0;
        int dm_maxpagesperfile = 0;
        int bm_buffercount = 0;
        String bm_policy = null;

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            StringBuilder json = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                json.append(line.trim());
            }

            String content = json.toString()
                    .replace("{", "")
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
                        case "bm_buffercount":
                            bm_buffercount = Integer.parseInt(value);
                            break;
                        case "bm_policy":
                            bm_policy = value;
                            break;
                    }
                }
            }
            return new DBConfig(dbpath, pagesize, dm_maxfilecount, dm_maxpagesperfile, bm_buffercount, bm_policy);
        } catch (IOException e) {
            System.out.println("Erreur lecture config: " + e.getMessage());
            return null;
        }
    }
}