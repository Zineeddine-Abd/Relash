package commandes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import Main.DBConfig;

public class Select {
    private String tableName;
    private DBConfig config;

    public Select(String tableName, DBConfig config) {
        this.tableName = tableName;
        this.config = config;
    }

    public void execute() {
        try {
            String tablePath = config.getDbPath() + "/" + tableName + ".tbl";

            BufferedReader reader = new BufferedReader(new FileReader(tablePath));
            String line;

            System.out.println("Contenu de la table " + tableName + " :");
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();

        } catch (IOException e) {
            System.out.println("Erreur lors de la lecture de la table : " + e.getMessage());
        }
    }
}
