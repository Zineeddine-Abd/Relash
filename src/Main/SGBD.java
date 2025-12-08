package Main;

import BufferManager.BufferManager;
import DBManager.DBManager;
import DiskManager.DiskManager;
import DiskManager.PageId;
import FileManager.Column;
import FileManager.Relation;

import java.io.File;
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
        // Initialisation
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
                    case "EXIT":
                        ProcessExitCommand();
                        running = false;
                        break;
                    default:
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

            // SUPPRIMER cette ligne selon consigne TP6 (pas d'affichage pour CREATE)
            // System.out.println("Table " + tableName + " créée avec succès.");

        } catch (Exception e) {
            System.err.println("Erreur création table : " + e.getMessage());
        }
    }

    private void ProcessDropTableCommand(String command) {
        String[] tokens = command.split("\\s+");
        if (tokens.length < 3)
            return;
        String tableName = tokens[2];

        Relation rel = dbManager.GetTable(tableName);
        if (rel != null) {
            // Désallouer les pages
            List<PageId> dataPages = rel.getDataPages();
            for (PageId pid : dataPages) {
                diskManager.DeallocPage(pid);
            }
            diskManager.DeallocPage(rel.getHeaderPageId());

            dbManager.RemoveTable(tableName);
            System.out.println("Table " + tableName + " supprimée.");
        } else {
            System.err.println("Table inconnue.");
        }
    }

    private void ProcessDropTablesCommand() {
        List<String> tableNames = dbManager.GetTableNames();
        for (String tableName : tableNames) {
            Relation rel = dbManager.GetTable(tableName);
            if (rel != null) {
                List<PageId> dataPages = rel.getDataPages();
                for (PageId pid : dataPages) {
                    diskManager.DeallocPage(pid);
                }
                diskManager.DeallocPage(rel.getHeaderPageId());
            }
        }
        dbManager.RemoveAllTables();
        System.out.println("Toutes les tables ont été supprimées.");
    }

    private void ProcessDescribeTableCommand(String command) {
        String[] tokens = command.split("\\s+");
        if (tokens.length < 3)
            return;
        String tableName = tokens[2];
        dbManager.DescribeTable(tableName);
    }

    private void ProcessExitCommand() {
        dbManager.SaveState();
        bufferManager.FlushBuffers();
        diskManager.Finish();
        System.out.println("Bye.");
    }
}