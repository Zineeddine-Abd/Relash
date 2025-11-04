package DiskManager;

import Main.DBConfig;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class DiskManagerTests {

    private static final String TEST_DB_PATH = "test_data";

    public static void main(String[] args) {
        System.out.println("--- Tests DiskManager ---\n");

        cleanup();

        testInit();
        testAllocPage();
        testWriteAndReadPage();
        testDeallocPage();
        testPersistence();

        // cleanup();

        System.out.println("\n--- Tous les tests terminés ---");
    }

    // Pour nettoyer l'environnement de test (supprimer les fichiers et les dossier
    // crees)
    private static void cleanup() {
        try {
            Path path = Paths.get(TEST_DB_PATH);
            if (Files.exists(path)) {
                Files.walk(path)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        } catch (IOException e) {
            System.err.println("Erreur lors du nettoyage: " + e.getMessage());
        }
    }

    public static void testInit() {
        System.out.println("Test 1: Init");
        cleanup();

        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 10, "LRU");
        DiskManager dm = new DiskManager(config);
        dm.Init();

        File binDataDir = new File(TEST_DB_PATH);
        if (binDataDir.exists() && binDataDir.isDirectory()) {
            System.out.println("Dossier de données cree avec succes !");
        } else {
            System.out.println("Echec creation dossier de donnees");
        }

        dm.Finish();
        System.out.println();
    }

    public static void testAllocPage() {
        System.out.println("Test 2: AllocPage");
        cleanup();

        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 10, "LRU");
        DiskManager dm = new DiskManager(config);
        dm.Init();

        PageId page1 = dm.AllocPage();
        PageId page2 = dm.AllocPage();
        PageId page3 = dm.AllocPage();

        System.out.println("Page 1 allouee: " + page1);
        System.out.println("Page 2 allouee: " + page2);
        System.out.println("Page 3 allouee: " + page3);

        if (page1.getPageIdx() == 0 && page2.getPageIdx() == 1 && page3.getPageIdx() == 2) {
            System.out.println("Allocation de pages séquentielle reussie");
        } else {
            System.out.println("Echec allocation de pages");
        }

        dm.Finish();
        System.out.println();
    }

    public static void testWriteAndReadPage() {
        System.out.println("Test 3: Write et Read Page");
        cleanup(); // Assurer un état propre

        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 10, "LRU");
        DiskManager dm = new DiskManager(config);
        dm.Init();

        PageId page = dm.AllocPage();

        ByteBuffer writeBuffer = ByteBuffer.allocate(4);
        writeBuffer.put((byte) 'A');
        writeBuffer.put((byte) 'B');
        writeBuffer.put((byte) 'C');
        writeBuffer.put((byte) 'D');
        writeBuffer.flip();

        dm.WritePage(page, writeBuffer);
        System.out.println("Donnees écrites: ABCD");

        ByteBuffer readBuffer = ByteBuffer.allocate(4);
        dm.ReadPage(page, readBuffer);

        byte[] readData = new byte[4];
        readBuffer.get(readData);

        String readString = new String(readData);
        System.out.println("Données lues: " + readString);

        if (readString.equals("ABCD")) {
            System.out.println("Lecture/Ecriture reussie");
        } else {
            System.out.println("Echec Lecture/Ecriture");
        }

        dm.Finish();
        System.out.println();
    }

    public static void testDeallocPage() {
        System.out.println("Test 4: DeallocPage");
        cleanup();

        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 10, "LRU");
        DiskManager dm = new DiskManager(config);
        dm.Init();

        PageId page1 = dm.AllocPage();
        System.out.println("Page allouee: " + page1);

        dm.DeallocPage(page1);
        System.out.println("Page désallouee: " + page1);

        PageId page2 = dm.AllocPage();
        System.out.println("Page réallouee: " + page2);

        if (page1.equals(page2)) {
            System.out.println("Reutilisation de page libre reussie");
        } else {
            System.out.println("Page libre non reutilisee");
        }

        dm.Finish();
        System.out.println();
    }

    public static void testPersistence() {
        System.out.println("Test 5: Persistence (Init/Finish)");
        cleanup();

        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 10, "LRU");
        PageId page;

        DiskManager dm1 = new DiskManager(config);
        dm1.Init();
        page = dm1.AllocPage();
        ByteBuffer writeBuffer = ByteBuffer.allocate(4);
        writeBuffer.put("TEST".getBytes());
        writeBuffer.flip();
        dm1.WritePage(page, writeBuffer);
        System.out.println("Donnees ecrites dans " + page);
        dm1.Finish();

        // relire les données
        DiskManager dm2 = new DiskManager(config);
        dm2.Init();
        ByteBuffer readBuffer = ByteBuffer.allocate(4);
        dm2.ReadPage(page, readBuffer);
        byte[] readData = new byte[4];
        readBuffer.get(readData);
        String readString = new String(readData);
        System.out.println("Donnees relues: " + readString);

        if (readString.equals("TEST")) {
            System.out.println("Persistance reussie");
        } else {
            System.out.println("ÉEhec persistance");
        }

        dm2.Finish();
        System.out.println();
    }
}