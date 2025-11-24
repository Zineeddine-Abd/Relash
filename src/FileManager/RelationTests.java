package FileManager;

import BufferManager.BufferManager;
import DiskManager.DiskManager;
import DiskManager.PageId;
import Main.DBConfig;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

public class RelationTests {

  private static final String TEST_DB_PATH = "test_tp5_data";
  private static DBConfig config;
  private static DiskManager dm;
  private static BufferManager bm;

  private static void setup() {
    cleanup();
    config = new DBConfig(TEST_DB_PATH, 1024, 4, 8, 5, "LRU");
    dm = new DiskManager(config);
    dm.Init();
    bm = new BufferManager(config, dm);
  }

  private static void cleanup() {
    try {
      if (bm != null)
        bm.FlushBuffers();
      if (dm != null)
        dm.Finish();

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

  public static void main(String[] args) {
    System.out.println("=== TP5 Relation/HeapFile Tests ===\n");
    setup();

    testInsertAndGetAll();
    testDeleteRecord();

    cleanup();
    System.out.println("\n=== TP5 Tests Passed ===");
  }

  public static void testInsertAndGetAll() {
    System.out.println("Test 1: InsertRecord et GetAllRecords");

    // 1. Allouer une Header Page pour notre relation
    PageId headerPageId = dm.AllocPage();
    // Initialiser la Header Page (listes vides)
    ByteBuffer headerBuf = bm.GetPage(headerPageId);
    headerBuf.putInt(0, -1); // Free Head FileIdx
    headerBuf.putInt(4, -1); // Free Head PageIdx
    headerBuf.putInt(8, -1); // Full Head FileIdx
    headerBuf.putInt(12, -1); // Full Head PageIdx
    bm.FreePage(headerPageId, true);

    // 2. Definir le schéma et créer la Relation
    Column[] cols = { new Column("id", ColumnType.INT), new Column("name", ColumnType.VARCHAR, 20) };
    Relation rel = new Relation("Users", cols, config, dm, bm, headerPageId);

    // 3. Inserer des records
    Record r1 = new Record(new Object[] { 1, "Alice" });
    Record r2 = new Record(new Object[] { 2, "Bob" });
    Record r3 = new Record(new Object[] { 3, "Charlie" });

    RecordId rid1 = rel.InsertRecord(r1);
    RecordId rid2 = rel.InsertRecord(r2);
    RecordId rid3 = rel.InsertRecord(r3);

    System.out.println("Inséré: " + rid1);
    System.out.println("Inséré: " + rid2);
    System.out.println("Inséré: " + rid3);

    // 4. Recupérer tous les records
    List<Record> all = rel.GetAllRecords();
    System.out.println("GetAllRecords a trouvé: " + all.size() + " records");

    assert all.size() == 3 : "Erreur: Nombre de records incorrect";
    assert all.get(0).equals(r1) || all.get(1).equals(r1) || all.get(2).equals(r1) : "Record 1 manquant";

    System.out.println("✓ Insert/Get All réussi\n");
  }

  public static void testDeleteRecord() {
    System.out.println("Test 2: DeleteRecord");

    // Setup
    PageId headerPageId = dm.AllocPage();
    ByteBuffer headerBuf = bm.GetPage(headerPageId);
    headerBuf.putInt(0, -1);
    headerBuf.putInt(4, -1);
    headerBuf.putInt(8, -1);
    headerBuf.putInt(12, -1);
    bm.FreePage(headerPageId, true);

    Column[] cols = { new Column("id", ColumnType.INT) };
    Relation rel = new Relation("Numbers", cols, config, dm, bm, headerPageId);

    Record r1 = new Record(new Object[] { 100 });
    Record r2 = new Record(new Object[] { 200 });

    RecordId rid1 = rel.InsertRecord(r1);
    RecordId rid2 = rel.InsertRecord(r2);

    System.out.println("Avant suppression: " + rel.GetAllRecords().size() + " records");
    assert rel.GetAllRecords().size() == 2 : "Erreur setup";

    // Supprimer r1
    rel.DeleteRecord(rid1);
    System.out.println("Après suppression de r1: " + rel.GetAllRecords().size() + " records");

    List<Record> all = rel.GetAllRecords();
    assert all.size() == 1 : "Erreur: Delete a échoué";
    assert all.get(0).equals(r2) : "Mauvais record supprimé";

    // Supprimer r2 (la page devrait devenir vide et etre desallouee)
    rel.DeleteRecord(rid2);
    System.out.println("Après suppression de r2: " + rel.GetAllRecords().size() + " records");
    assert rel.GetAllRecords().isEmpty() : "Erreur: Page non vidée";

    System.out.println("✓ Delete réussi\n");
  }
}