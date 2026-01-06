package DBManager;

import FileManager.ColumnType;
import FileManager.Relation;
import FileManager.Column;
import DiskManager.DiskManager;
import BufferManager.BufferManager;
import DiskManager.PageId;
import Main.DBConfig;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class DBManager {

    private DBConfig config;
    private Map<String, Relation> tables;

    public DBManager(DBConfig config) {
        this.config = config;
        this.tables = new HashMap<>();
    }

    public void AddTable(Relation tab) {
        if (tables.containsKey(tab.getRelationName())) {
            System.err.println("Erreur : La table " + tab.getRelationName() + " existe déjà.");
            return;
        }
        tables.put(tab.getRelationName(), tab);
    }

    public Relation GetTable(String nomTable) {
        return tables.get(nomTable);
    }

    public void RemoveTable(String nomTable) {
        if (tables.containsKey(nomTable)) {
            tables.remove(nomTable);
        } else {
            System.err.println("Erreur : Table " + nomTable + " introuvable.");
        }
    }

    public void RemoveAllTables() {
        tables.clear();
    }

    public List<String> GetTableNames() {
        return new ArrayList<>(tables.keySet());
    }

    public void DescribeTable(String nomTable) {
        Relation rel = tables.get(nomTable);
        if (rel == null) {
            System.err.println("Erreur : Table " + nomTable + " introuvable.");
            return;
        }
        printTableSchema(rel);
    }

    public void DescribeAllTables() {
        for (Relation rel : tables.values()) {
            printTableSchema(rel);
        }
    }

    private void printTableSchema(Relation rel) {
        StringBuilder sb = new StringBuilder();
        sb.append(rel.getRelationName()).append(" (");
        Column[] cols = rel.getColumns();

        for (int i = 0; i < cols.length; i++) {
            sb.append(cols[i].getColumnName()).append(":");

            ColumnType type = cols[i].getColumnType();

            if (type == ColumnType.CHAR || type == ColumnType.VARCHAR) {
                sb.append(type.toString())
                        .append("(")
                        .append(cols[i].getSizeInBytes())
                        .append(")");
            } else {
                sb.append(type.toString());
            }

            if (i < cols.length - 1) {
                sb.append(",");
            }
        }
        sb.append(")");
        System.out.println(sb.toString());
    }

    public void SaveState() {
        String savePath = config.getDbPath() + File.separator + "tables.save";
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(savePath))) {
            oos.writeInt(tables.size());

            for (Relation rel : tables.values()) {
                oos.writeUTF(rel.getRelationName());
                oos.writeObject(rel.getHeaderPageId());
                oos.writeObject(rel.getColumns());
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de la sauvegarde de l'état : " + e.getMessage());
        }
    }

    // Signature modifiée pour simplifier la reconstruction des tables
    public void LoadState(DiskManager dm, BufferManager bm) {
        String savePath = config.getDbPath() + File.separator + "tables.save";
        File file = new File(savePath);
        if (!file.exists() || file.length() == 0)
            return;

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            int tableCount = ois.readInt();

            for (int i = 0; i < tableCount; i++) {
                String name = ois.readUTF();
                PageId headerId = (PageId) ois.readObject();
                Column[] cols = (Column[]) ois.readObject();

                // Reconstruction directe grâce aux paramètres dm et bm
                Relation rel = new Relation(name, cols, config, dm, bm, headerId);
                this.tables.put(name, rel);
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Erreur chargement état : " + e.getMessage());
        }
    }
}