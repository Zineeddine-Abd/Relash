package commandes;

import java.io.FileWriter;
import java.io.IOException;

import main.DBConfig;

public class Insert {
    private String tableName;
    private String[] values;
    private DBConfig config;

    public Insert(String tableName, String[] values, DBConfig config) {
        this.tableName = tableName;
        this.values = values;
        this.config = config;
    }

    public void execute() {
        try {
            String tablePath = config.getDbPath() + "/" + tableName + ".tbl";

            FileWriter writer = new FileWriter(tablePath, true);
            writer.write(String.join(",", values) + "\n");
            writer.close();

            System.out.println("Valeurs inserees dans la table " + tableName + " avec succes !");
        } catch (IOException e) {
            System.out.println("Erreur lors de l'insertion : " + e.getMessage());
        }
    }
}
