package Main;

import BufferManager.BufferManager;
import DBManager.DBManager;
import DiskManager.DiskManager;
import DiskManager.PageId;
import FileManager.Column;
import FileManager.Relation;
import FileManager.Record;
import FileManager.RecordId;
import FileManager.ColumnType;
import QueryManager.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class SGBD {

    private DBConfig config;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private DBManager dbManager;

    public SGBD(DBConfig config) {
        this.config = config;
        this.diskManager = new DiskManager(config);
        this.bufferManager = new BufferManager(config, diskManager);
        this.dbManager = new DBManager(config);
    }

    public static void main(String[] args) {
        String configPath = (args.length > 0) ? args[0] : "config/dbconfig.json";
        DBConfig config = DBConfig.loadFromFile(configPath);

        if (config == null) {
            System.err.println("Impossible de charger la configuration.");
            return;
        }

        SGBD sgbd = new SGBD(config);
        sgbd.Run();
    }

    public void Run() {
        diskManager.Init();
        dbManager.LoadState(diskManager, bufferManager);

        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            if (scanner.hasNextLine()) {
                String commandLine = scanner.nextLine().trim();
                if (commandLine.isEmpty())
                    continue;

                String[] tokens = commandLine.split("\\s+");
                String firstWord = tokens[0].toUpperCase();

                switch (firstWord) {
                    case "CREATE":
                        if (tokens.length > 1 && tokens[1].equalsIgnoreCase("TABLE")) {
                            ProcessCreateTableCommand(commandLine);
                        }
                        break;
                    case "DROP":
                        if (tokens.length > 1 && tokens[1].equalsIgnoreCase("TABLE")) {
                            ProcessDropTableCommand(commandLine);
                        } else if (tokens.length > 1 && tokens[1].equalsIgnoreCase("TABLES")) {
                            ProcessDropTablesCommand();
                        }
                        break;
                    case "DESCRIBE":
                        if (tokens.length > 1 && tokens[1].equalsIgnoreCase("TABLE")) {
                            ProcessDescribeTableCommand(commandLine);
                        } else if (tokens.length > 1 && tokens[1].equalsIgnoreCase("TABLES")) {
                            dbManager.DescribeAllTables();
                        }
                        break;
                    case "INSERT":
                        ProcessInsertCommand(commandLine);
                        break;
                    case "APPEND":
                        ProcessAppendCommand(commandLine);
                        break;
                    case "SELECT":
                        ProcessSelectCommand(commandLine);
                        break;
                    case "DELETE":
                        ProcessDeleteCommand(commandLine);
                        break;
                    case "UPDATE":
                        ProcessUpdateCommand(commandLine);
                        break;
                    case "EXIT":
                        ProcessExitCommand();
                        running = false;
                        break;
                }
            }
        }
        scanner.close();
    }

    private void ProcessCreateTableCommand(String command) {
        try {
            int openParenIndex = command.indexOf('(');
            if (openParenIndex == -1)
                return;

            String beforeParen = command.substring(0, openParenIndex).trim();
            String tableName = beforeParen.substring("CREATE TABLE".length()).trim();

            int closeParenIndex = command.lastIndexOf(')');
            String columnsStr = command.substring(openParenIndex + 1, closeParenIndex);

            String[] colDefs = columnsStr.split(",");
            List<Column> columnsList = new ArrayList<>();

            for (String colDef : colDefs) {
                String[] nameType = colDef.trim().split(":");
                String colName = nameType[0].trim();
                String colTypeStr = nameType[1].trim();
                columnsList.add(new Column(colName, colTypeStr));
            }

            Column[] columns = columnsList.toArray(new Column[0]);

            PageId headerPageId = diskManager.AllocPage();
            ByteBuffer headerBuff = bufferManager.GetPage(headerPageId);
            headerBuff.putInt(0, -1);
            headerBuff.putInt(4, -1);
            headerBuff.putInt(8, -1);
            headerBuff.putInt(12, -1);
            bufferManager.FreePage(headerPageId, true);

            Relation rel = new Relation(tableName, columns, config, diskManager, bufferManager, headerPageId);
            dbManager.AddTable(rel);
        } catch (Exception e) {
            System.err.println("Erreur creation table : " + e.getMessage());
        }
    }

    private void ProcessDropTableCommand(String command) {
        String[] tokens = command.split("\\s+");
        if (tokens.length < 3)
            return;
        String tableName = tokens[2];
        Relation rel = dbManager.GetTable(tableName);
        if (rel != null) {
            for (PageId pid : rel.getDataPages())
                diskManager.DeallocPage(pid);
            diskManager.DeallocPage(rel.getHeaderPageId());
            dbManager.RemoveTable(tableName);
        }
    }

    private void ProcessDropTablesCommand() {
        for (String tableName : dbManager.GetTableNames()) {
            Relation rel = dbManager.GetTable(tableName);
            if (rel != null) {
                for (PageId pid : rel.getDataPages())
                    diskManager.DeallocPage(pid);
                diskManager.DeallocPage(rel.getHeaderPageId());
            }
        }
        dbManager.RemoveAllTables();
    }

    private void ProcessDescribeTableCommand(String command) {
        String[] tokens = command.split("\\s+");
        if (tokens.length < 3)
            return;
        dbManager.DescribeTable(tokens[2]);
    }

    // INSERT INTO NomRel VALUES (v1, v2...)
    private void ProcessInsertCommand(String command) {
        try {
            String temp = command.substring("INSERT INTO".length()).trim();
            int valuesIdx = temp.toUpperCase().indexOf("VALUES");
            String tableName = temp.substring(0, valuesIdx).trim();

            String valuesPart = temp.substring(valuesIdx + "VALUES".length()).trim();
            valuesPart = valuesPart.substring(1, valuesPart.length() - 1); // enlever ()

            Relation rel = dbManager.GetTable(tableName);
            if (rel == null) {
                System.out.println("Table inconnue");
                return;
            }

            String[] rawValues = valuesPart.split(",");
            Record record = new Record(rel.getColumns().length);

            for (int i = 0; i < rawValues.length; i++) {
                parseAndSetRecordValue(record, i, rawValues[i].trim(), rel.getColumns()[i].getColumnType());
            }

            rel.InsertRecord(record);

        } catch (Exception e) {
            System.err.println("Erreur INSERT: " + e.getMessage());
        }
    }

    // APPEND INTO NomRel ALLRECORDS (fichier.csv)
    private void ProcessAppendCommand(String command) {
        try {
            String temp = command.substring("APPEND INTO".length()).trim();
            int allRecIdx = temp.toUpperCase().indexOf("ALLRECORDS");
            String tableName = temp.substring(0, allRecIdx).trim();

            String filePart = temp.substring(allRecIdx + "ALLRECORDS".length()).trim();
            filePart = filePart.substring(1, filePart.length() - 1); // enlever ()

            Relation rel = dbManager.GetTable(tableName);
            if (rel == null)
                return;

            File csvFile = new File(filePart);
            if (!csvFile.exists()) {
                System.out.println("Fichier " + filePart + " introuvable à la racine.");
                return;
            }

            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty())
                    continue;
                String[] parts = line.split(",");
                Record record = new Record(rel.getColumns().length);
                for (int i = 0; i < parts.length; i++) {
                    parseAndSetRecordValue(record, i, parts[i].trim(), rel.getColumns()[i].getColumnType());
                }
                rel.InsertRecord(record);
                count++;
            }
            br.close();
            System.out.println("Records ajoutés : " + count);

        } catch (Exception e) {
            System.err.println("Erreur APPEND : " + e.getMessage());
        }
    }

    private void parseAndSetRecordValue(Record rec, int index, String val, ColumnType type) {
        switch (type) {
            case INT:
                rec.setValue(index, Integer.parseInt(val));
                break;
            case FLOAT:
                rec.setValue(index, Float.parseFloat(val));
                break;
            case CHAR:
            case VARCHAR:
                if (val.startsWith("\"") && val.endsWith("\"")) {
                    val = val.substring(1, val.length() - 1);
                }
                rec.setValue(index, val);
                break;
        }
    }

    // SELECT ... FROM ... WHERE ...
    private void ProcessSelectCommand(String command) {
        try {
            int fromIdx = command.toUpperCase().indexOf(" FROM ");
            if (fromIdx == -1)
                return;

            String selectPart = command.substring("SELECT".length(), fromIdx).trim();
            String rest = command.substring(fromIdx + " FROM ".length()).trim();

            int whereIdx = rest.toUpperCase().indexOf(" WHERE ");
            String fromPart = (whereIdx == -1) ? rest : rest.substring(0, whereIdx).trim();
            String wherePart = (whereIdx == -1) ? null : rest.substring(whereIdx + " WHERE ".length()).trim();

            String[] tableAlias = fromPart.split("\\s+");
            String tableName = tableAlias[0];
            String alias = (tableAlias.length > 1) ? tableAlias[1] : "";

            Relation rel = dbManager.GetTable(tableName);
            if (rel == null) {
                System.out.println("Table inconnue");
                return;
            }

            // Construire l'itérateur de base
            IRecordIterator iterator = new RelationScanner(rel, bufferManager);

            // Gestion WHERE
            if (wherePart != null) {
                List<Condition> conditions = parseConditions(wherePart, rel, alias);
                iterator = new SelectOperator(iterator, conditions);
            }

            // Gestion SELECT (Projection)
            List<Integer> projIndices = new ArrayList<>();
            if (!selectPart.equals("*")) {
                String[] cols = selectPart.split(",");
                for (String col : cols) {
                    col = col.trim();
                    if (col.contains("."))
                        col = col.split("\\.")[1]; // remove alias
                    int idx = rel.getColumnIndex(col);
                    if (idx != -1)
                        projIndices.add(idx);
                }
                iterator = new ProjectOperator(iterator, projIndices);
            }

            // Affichage
            new RecordPrinter(iterator).print();

        } catch (Exception e) {
            System.err.println("Erreur SELECT: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // DELETE Rel Alias WHERE ...
    private void ProcessDeleteCommand(String command) {
        try {
            String temp = command.substring("DELETE".length()).trim();
            int whereIdx = temp.toUpperCase().indexOf("WHERE");
            String tablePart = (whereIdx == -1) ? temp : temp.substring(0, whereIdx).trim();
            String wherePart = (whereIdx == -1) ? null : temp.substring(whereIdx + "WHERE".length()).trim();

            String[] tableAlias = tablePart.split("\\s+");
            String tableName = tableAlias[0];
            String alias = (tableAlias.length > 1) ? tableAlias[1] : "";

            Relation rel = dbManager.GetTable(tableName);
            if (rel == null)
                return;

            IRecordIterator iterator = new RelationScanner(rel, bufferManager);
            if (wherePart != null) {
                List<Condition> conds = parseConditions(wherePart, rel, alias);
                iterator = new SelectOperator(iterator, conds);
            }

            List<RecordId> toDelete = new ArrayList<>();
            Record rec;
            while ((rec = iterator.GetNextRecord()) != null) {
                toDelete.add(rec.getRid());
            }

            // a l'inverse pour évité les problèmes d'index ou de désallocation séquentielle
            // sur une même page
            for (int i = toDelete.size() - 1; i >= 0; i--) {
                rel.DeleteRecord(toDelete.get(i));
            }

            System.out.println("Total deleted records = " + toDelete.size());

        } catch (Exception e) {
            System.err.println("Erreur DELETE: " + e.getMessage());
        }
    }

    // UPDATE Rel Alias SET col=val, ... WHERE ...
    private void ProcessUpdateCommand(String command) {
        try {
            String temp = command.substring("UPDATE".length()).trim();
            int setIdx = temp.toUpperCase().indexOf(" SET ");
            String tablePart = temp.substring(0, setIdx).trim();
            String rest = temp.substring(setIdx + " SET ".length()).trim();

            int whereIdx = rest.toUpperCase().indexOf(" WHERE ");
            String setPart = (whereIdx == -1) ? rest : rest.substring(0, whereIdx).trim();
            String wherePart = (whereIdx == -1) ? null : rest.substring(whereIdx + " WHERE ".length()).trim();

            String[] tableAlias = tablePart.split("\\s+");
            String tableName = tableAlias[0];
            String alias = (tableAlias.length > 1) ? tableAlias[1] : "";

            Relation rel = dbManager.GetTable(tableName);
            if (rel == null)
                return;

            // Parsing SET clause
            String[] updates = setPart.split(",");
            List<Integer> updateCols = new ArrayList<>();
            List<String> updateVals = new ArrayList<>();

            for (String up : updates) {
                String[] kv = up.split("=");
                String col = kv[0].trim();
                if (col.contains("."))
                    col = col.split("\\.")[1];
                updateCols.add(rel.getColumnIndex(col));
                updateVals.add(kv[1].trim());
            }

            IRecordIterator iterator = new RelationScanner(rel, bufferManager);
            if (wherePart != null) {
                List<Condition> conds = parseConditions(wherePart, rel, alias);
                iterator = new SelectOperator(iterator, conds);
            }

            int count = 0;
            Record rec;
            List<Record> recordsToUpdate = new ArrayList<>();
            while ((rec = iterator.GetNextRecord()) != null) {
                recordsToUpdate.add(rec);
            }

            for (Record r : recordsToUpdate) {
                // Appliquer les modifs
                for (int i = 0; i < updateCols.size(); i++) {
                    int colIdx = updateCols.get(i);
                    String valStr = updateVals.get(i);
                    parseAndSetRecordValue(r, colIdx, valStr, rel.getColumns()[colIdx].getColumnType());
                }
                rel.updateRecord(r.getRid(), r);
                count++;
            }
            System.out.println("Total updated records = " + count);

        } catch (Exception e) {
            System.err.println("Erreur UPDATE: " + e.getMessage());
        }
    }

    // Helper pour parser les conditions
    private List<Condition> parseConditions(String wherePart, Relation rel, String alias) {
        List<Condition> conditions = new ArrayList<>();
        String[] condsStr = wherePart.split(" AND ");

        for (String c : condsStr) {
            c = c.trim();
            // Trouver l'opérateur
            String op = "";
            if (c.contains("<="))
                op = "<=";
            else if (c.contains(">="))
                op = ">=";
            else if (c.contains("<>"))
                op = "<>";
            else if (c.contains("="))
                op = "=";
            else if (c.contains("<"))
                op = "<";
            else if (c.contains(">"))
                op = ">";

            if (op.isEmpty())
                continue;

            String[] parts = c.split(op);
            String left = parts[0].trim();
            String right = parts[1].trim();

            if (left.contains("."))
                left = left.split("\\.")[1];

            int colLeft = rel.getColumnIndex(left);
            ColumnType type = rel.getColumns()[colLeft].getColumnType();

            // Vérifier si droite est colonne ou valeur
            boolean rightIsCol = right.contains(alias + ".");
            if (rightIsCol) {
                String rightColName = right.split("\\.")[1];
                int colRight = rel.getColumnIndex(rightColName);
                conditions.add(new Condition(colLeft, op, colRight, type));
            } else {
                conditions.add(new Condition(colLeft, op, right, type));
            }
        }
        return conditions;
    }

    private void ProcessExitCommand() {
        dbManager.SaveState();
        bufferManager.FlushBuffers();
        diskManager.Finish();
    }
}