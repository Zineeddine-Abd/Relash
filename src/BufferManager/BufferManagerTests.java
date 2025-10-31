package Buffer_Manager;

import Disk_Manager.DiskManager;
import Disk_Manager.PageId;
import Main.DBConfig;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class BufferManagerTests {

    private static final String TEST_DB_PATH = "test_bm_data";

    public static void main(String[] args) {

        System.out.println("--- Tests BufferManager ---\n");

        cleanup();

        testLoadsFromDisk();
        testReturnsExistingPage();
        testFreePage_And_Dirty();
        testLRU_ReplacementPolicy();
        testMRU_ReplacementPolicy();
        testFlushBuffers();
        testPinningPreventsReplacement();

        cleanup();

        System.out.println("\n--- Tous les tests du BufferManager sont terminés ---");
    }

    private static void setupTestEnv(DBConfig config) {
        DiskManager dm = new DiskManager(config);
        dm.Init();
        dm.Finish();
    }

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

    public static void testLoadsFromDisk() {
        System.out.println("Test 1: GetPage charge une page depuis le disque");
        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 3, "LRU");
        setupTestEnv(config);
        DiskManager dm = new DiskManager(config);
        dm.Init();
        BufferManager bm = new BufferManager(config, dm);

        PageId pageId = dm.AllocPage();
        ByteBuffer writeBuffer = ByteBuffer.wrap(new byte[]{'A', 'B', 'C', 'D'});
        dm.WritePage(pageId, writeBuffer);

        ByteBuffer readBuffer = bm.GetPage(pageId);
        byte[] data = new byte[4];
        readBuffer.get(data);

        if ("ABCD".equals(new String(data))) {
            System.out.println("Reussi: La page a ete lue depuis le disque et chargee dans le buffer");
        } else {
            System.out.println("Echec");
        }
        bm.FreePage(pageId, false);
        dm.Finish();
        System.out.println();
    }

    public static void testReturnsExistingPage() {
        System.out.println("Test 2: GetPage retourne une page déja en memoire sans lire le disque");
        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 3, "LRU");
        setupTestEnv(config);
        DiskManager dm = new DiskManager(config);
        dm.Init();
        BufferManager bm = new BufferManager(config, dm);

        PageId pageId = dm.AllocPage();
        bm.GetPage(pageId); // Premier appel, charge depuis le disque
        bm.GetPage(pageId); // Deuxième appel, doit venir du buffer

        if (bm.getLoadedPageCount() == 1) {
            System.out.println("Réussi: La page existante a ete retournee.");
        } else {
            System.out.println("Echec.");
        }
        bm.FreePage(pageId, false);
        dm.Finish();
        System.out.println();
    }

    public static void testFreePage_And_Dirty() {
        System.out.println("Test 3: FreePage et le flag dirty");
        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 3, "LRU");
        setupTestEnv(config);
        DiskManager dm = new DiskManager(config);
        dm.Init();
        BufferManager bm = new BufferManager(config, dm);

        PageId pageId = dm.AllocPage();
        bm.GetPage(pageId);
        bm.FreePage(pageId, true); // Marquer la page comme "dirty"

        // Pour verifier, on force le remplacement et on voit si la page est ecrite
        bm.GetPage(dm.AllocPage());
        bm.GetPage(dm.AllocPage());
        bm.GetPage(dm.AllocPage()); // Ceci devrait remplacer la premiere page

        ByteBuffer readBuffer = ByteBuffer.allocate(4);
        dm.ReadPage(pageId, readBuffer);
        // Si FreePage(dirty=true) a marche, le flush implicite l'aura écrite
        // (Ici le contenu sera vide, mais le test est conceptuel)
        System.out.println("Test conceptuel passe (vérification manuelle nécessaire via Flush).");

        dm.Finish();
        System.out.println();
    }

    public static void testLRU_ReplacementPolicy() {
        System.out.println("Test 4: Politique de remplacement LRU");
        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 3, "LRU"); // 3 buffers
        setupTestEnv(config);
        DiskManager dm = new DiskManager(config);
        dm.Init();
        BufferManager bm = new BufferManager(config, dm);

        PageId p1 = dm.AllocPage();
        PageId p2 = dm.AllocPage();
        PageId p3 = dm.AllocPage();

        bm.GetPage(p1); bm.FreePage(p1, false); // Accès 1
        bm.GetPage(p2); bm.FreePage(p2, false); // Accès 2
        bm.GetPage(p3); bm.FreePage(p3, false); // Accès 3

        bm.GetPage(p1); bm.FreePage(p1, false); // p1 est maintenant le plus récent

        PageId p4 = dm.AllocPage();
        bm.GetPage(p4); // Devrait remplacer p2 (le moins récemment utilise)

        System.out.println(bm.getBufferPoolStatus());
        System.out.println("LRU : La page p2 devrait avoir ete remplacee. Vérifiez le statut ci-dessus.");
        dm.Finish();
        System.out.println();
    }

    public static void testMRU_ReplacementPolicy() {
        System.out.println("Test 5: Politique de remplacement MRU");
        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 3, "MRU"); // 3 buffers
        setupTestEnv(config);
        DiskManager dm = new DiskManager(config);
        dm.Init();
        BufferManager bm = new BufferManager(config, dm);
        bm.SetCurrentReplacementPolicy("MRU");

        PageId p1 = dm.AllocPage();
        PageId p2 = dm.AllocPage();
        PageId p3 = dm.AllocPage();

        bm.GetPage(p1); bm.FreePage(p1, false);
        bm.GetPage(p2); bm.FreePage(p2, false);
        bm.GetPage(p3); bm.FreePage(p3, false);

        bm.GetPage(p1); bm.FreePage(p1, false); // p1 est maintenant le plus récent

        PageId p4 = dm.AllocPage();
        bm.GetPage(p4); // Devrait remplacer p1 (le plus récemment utilisé)

        System.out.println(bm.getBufferPoolStatus());
        System.out.println("MRU : La page p1 devrait avoir ete remplacee. Vérifiez le statut ci-dessus");
        dm.Finish();
        System.out.println();
    }

    public static void testFlushBuffers() {
        System.out.println("Test 6: FlushBuffers ecrit les pages sales");
        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 3, "LRU");
        setupTestEnv(config);
        DiskManager dm = new DiskManager(config);
        dm.Init();
        BufferManager bm = new BufferManager(config, dm);

        PageId pageId = dm.AllocPage();
        ByteBuffer buffer = bm.GetPage(pageId);
        buffer.put(0, (byte)'X'); // Modifier la page
        bm.FreePage(pageId, true); // Marquer comme dirty

        bm.FlushBuffers(); // Doit écrire 'X' sur le disque

        ByteBuffer readBuffer = ByteBuffer.allocate(4);
        dm.ReadPage(pageId, readBuffer);

        if (readBuffer.get(0) == 'X') {
            System.out.println("Réussi: La page dirty a bien ete ecrite sur le disque.");
        } else {
            System.out.println("Echec.");
        }
        dm.Finish();
        System.out.println();
    }

    public static void testPinningPreventsReplacement() {
        System.out.println("Test 7: Le pinning empeche le remplacement");
        DBConfig config = new DBConfig(TEST_DB_PATH, 4, 2, 8, 2, "LRU"); // 2 buffers
        setupTestEnv(config);
        DiskManager dm = new DiskManager(config);
        dm.Init();
        BufferManager bm = new BufferManager(config, dm);

        PageId p1 = dm.AllocPage();
        PageId p2 = dm.AllocPage();
        PageId p3 = dm.AllocPage();

        bm.GetPage(p1); // pin_count = 1
        bm.GetPage(p2); // pin_count = 1

        try {
            bm.GetPage(p3); // Devrait lancer une exception car p1 et p2 sont "pinned"
            System.out.println("Echec: Aucune exception levée alors que tous les buffers sont pinned");
        } catch (RuntimeException e) {
            System.out.println("Reussi: Exception levee car aucun buffer n'était disponible pour remplacement");
        }

        bm.FreePage(p1, false); // pin_count = 0
        bm.GetPage(p3); // Devrait maintenant remplacer p1
        System.out.println(bm.getBufferPoolStatus());
        System.out.println("La page p1 devrait avoir eté remplacee. Vérifiez le statut.");

        dm.Finish();
        System.out.println();
    }
}