package commandes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import Main.DBConfig;

public class CreateTable {
    private String tableName;
    private String[] columns;
    private DBConfig config;

    public CreateTable(String tableName, String[] columns, DBConfig config) {
        this.tableName = tableName;
        this.columns = columns;
        this.config = config;
    }

    public void execute() {
        try {
            File folder = new File(config.getDbPath());

            File tableFile = new File(config.getDbPath() + "/" + tableName + ".tbl");

            if (tableFile.createNewFile()) {
                FileWriter writer = new FileWriter(tableFile);
                writer.write(String.join(",", columns) + "\n");
                writer.close();
                System.out.println("Table " + tableName + " cree avec succes !");
            } else {
                System.out.println("La table " + tableName + " existe déja !");
            }
        } catch (IOException e) {
            System.out.println("Erreur lors de la création de la table : " + e.getMessage());
        }
    }
}
