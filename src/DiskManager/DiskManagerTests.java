package DiskManager;

import Main.DBConfig;

import java.io.File;
import java.nio.ByteBuffer;

public class DiskManagerTests {

    public static void main(String[] args) {
        System.out.println("=== Tests DiskManager ===\n");

        testInit();
        testAllocPage();
        testWriteAndReadPage();
        testDeallocPage();
        testPersistence();

        System.out.println("\n=== Tous les tests terminés ===");
    }

    public static void testInit() {
        System.out.println("Test 1: Init");

        DBConfig config = new DBConfig("test_data", 4, 2, 8);
        DiskManager dm = new DiskManager(config);
        dm.Init();

        File binDataDir = new File("test_data/BinData");
        if (binDataDir.exists() && binDataDir.isDirectory()) {
            System.out.println("✓ Dossier BinData créé avec succès");
        } else {
            System.out.println("✗ Échec création dossier BinData");
        }

        dm.Finish();
        System.out.println();
    }

    public static void testAllocPage() {
        System.out.println("Test 2: AllocPage");

        DBConfig config = new DBConfig("test_data", 4, 2, 8);
        DiskManager dm = new DiskManager(config);
        dm.Init();

        PageId page1 = dm.AllocPage();
        PageId page2 = dm.AllocPage();
        PageId page3 = dm.AllocPage();

        System.out.println("Page 1 allouée: " + page1);
        System.out.println("Page 2 allouée: " + page2);
        System.out.println("Page 3 allouée: " + page3);

        if (page1 != null && page2 != null && page3 != null) {
            System.out.println("✓ Allocation de pages réussie");
        } else {
            System.out.println("✗ Échec allocation de pages");
        }

        dm.Finish();
        System.out.println();
    }

    public static void testWriteAndReadPage() {
        System.out.println("Test 3: Write et Read Page");

        DBConfig config = new DBConfig("test_data", 4, 2, 8);
        DiskManager dm = new DiskManager(config);
        dm.Init();

        PageId page = dm.AllocPage();

        // Écrire des données
        ByteBuffer writeBuffer = ByteBuffer.allocate(4);
        writeBuffer.put((byte) 'A');
        writeBuffer.put((byte) 'B');
        writeBuffer.put((byte) 'C');
        writeBuffer.put((byte) 'D');
        writeBuffer.flip();

        dm.WritePage(page, writeBuffer);
        System.out.println("Données écrites: ABCD");

        // Lire les données
        ByteBuffer readBuffer = ByteBuffer.allocate(4);
        dm.ReadPage(page, readBuffer);

        readBuffer.flip();
        byte[] readData = new byte[4];
        readBuffer.get(readData);

        String readString = new String(readData);
        System.out.println("Données lues: " + readString);

        if (readString.equals("ABCD")) {
            System.out.println("✓ Lecture/Écriture réussie");
        } else {
            System.out.println("✗ Échec Lecture/Écriture");
        }

        dm.Finish();
        System.out.println();
    }

    public static void testDeallocPage() {
        System.out.println("Test 4: DeallocPage");

        DBConfig config = new DBConfig("test_data", 4, 2, 8);
        DiskManager dm = new DiskManager(config);
        dm.Init();

        PageId page1 = dm.AllocPage();
        System.out.println("Page allouée: " + page1);

        dm.DeallocPage(page1);
        System.out.println("Page désallouée: " + page1);

        PageId page2 = dm.AllocPage();
        System.out.println("Page réallouée: " + page2);

        if (page1.equals(page2)) {
            System.out.println("✓ Réutilisation de page libre réussie");
        } else {
            System.out.println("✗ Page libre non réutilisée");
        }

        dm.Finish();
        System.out.println();
    }

    public static void testPersistence() {
        System.out.println("Test 5: Persistence (Init/Finish)");

        DBConfig config = new DBConfig("test_data", 4, 2, 8);

        // Premier cycle: écrire des données
        DiskManager dm1 = new DiskManager(config);
        dm1.Init();

        PageId page = dm1.AllocPage();
        ByteBuffer writeBuffer = ByteBuffer.allocate(4);
        writeBuffer.put((byte) 'T');
        writeBuffer.put((byte) 'E');
        writeBuffer.put((byte) 'S');
        writeBuffer.put((byte) 'T');
        writeBuffer.flip();

        dm1.WritePage(page, writeBuffer);
        System.out.println("Données écrites dans " + page);
        dm1.Finish();

        // Deuxième cycle: relire les données
        DiskManager dm2 = new DiskManager(config);
        dm2.Init();

        ByteBuffer readBuffer = ByteBuffer.allocate(4);
        dm2.ReadPage(page, readBuffer);

        readBuffer.flip();
        byte[] readData = new byte[4];
        readBuffer.get(readData);

        String readString = new String(readData);
        System.out.println("Données relues: " + readString);

        if (readString.equals("TEST")) {
            System.out.println("✓ Persistance réussie");
        } else {
            System.out.println("✗ Échec persistance");
        }

        dm2.Finish();
        System.out.println();
    }
}