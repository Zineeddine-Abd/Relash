package FileManager;

import java.nio.ByteBuffer;

public class RelationTests {

  public static void main(String[] args) {
    System.out.println("=== TP4 Buffer Operations Tests ===\n");

    testIntFloat();
    testChar();
    testVarchar();
    testMixedTypes();
    testMultipleRecords();

    System.out.println("\n=== All tests passed ===");
  }

  private static void testIntFloat() {
    System.out.println("Test 1: INT and FLOAT types");

    Column[] cols = { new Column("id", ColumnType.INT), new Column("price", ColumnType.FLOAT) };
    Relation rel = new Relation("Products", cols);

    Record original = new Record(new Object[] { 123, 45.67f });
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    rel.writeRecordToBuffer(original, buffer, 0);

    Record read = new Record(2);
    rel.readFromBuffer(read, buffer, 0);

    assert read.getValue(0).equals(123) : "INT value mismatch";
    assert Math.abs((Float) read.getValue(1) - 45.67f) < 0.001 : "FLOAT value mismatch";

    System.out.println("✓ INT/FLOAT write-read successful");
    System.out.println("  Original: " + original);
    System.out.println("  Read:     " + read + "\n");
  }

  private static void testChar() {
    System.out.println("Test 2: CHAR(10) with padding");

    Column[] cols = { new Column("code", ColumnType.CHAR, 10) };
    Relation rel = new Relation("Codes", cols);

    Record original = new Record(new Object[] { "ABC" });
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    rel.writeRecordToBuffer(original, buffer, 0);

    Record read = new Record(1);
    rel.readFromBuffer(read, buffer, 0);

    assert read.getValue(0).equals("ABC") : "CHAR value mismatch";

    System.out.println("✓ CHAR padding and trim successful");
    System.out.println("  Original: " + original);
    System.out.println("  Read:     " + read + "\n");
  }

  private static void testVarchar() {
    System.out.println("Test 3: VARCHAR(20) variable length");

    Column[] cols = { new Column("name", ColumnType.VARCHAR, 20) };
    Relation rel = new Relation("Users", cols);

    Record original = new Record(new Object[] { "John Doe" });
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    rel.writeRecordToBuffer(original, buffer, 0);

    Record read = new Record(1);
    rel.readFromBuffer(read, buffer, 0);

    assert read.getValue(0).equals("John Doe") : "VARCHAR value mismatch";

    System.out.println("✓ VARCHAR write-read successful");
    System.out.println("  Original: " + original);
    System.out.println("  Read:     " + read + "\n");
  }

  private static void testMixedTypes() {
    System.out.println("Test 4: Mixed types - INT, FLOAT, CHAR, VARCHAR");

    Column[] cols = { new Column("id", ColumnType.INT), new Column("salary", ColumnType.FLOAT),
        new Column("code", ColumnType.CHAR, 5), new Column("name", ColumnType.VARCHAR, 15) };
    Relation rel = new Relation("Employees", cols);

    Record original = new Record(new Object[] { 42, 5000.50f, "EMP01", "Alice" });
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    rel.writeRecordToBuffer(original, buffer, 0);

    Record read = new Record(4);
    rel.readFromBuffer(read, buffer, 0);

    assert read.getValue(0).equals(42) : "INT mismatch";
    assert Math.abs((Float) read.getValue(1) - 5000.50f) < 0.01 : "FLOAT mismatch";
    assert read.getValue(2).equals("EMP01") : "CHAR mismatch";
    assert read.getValue(3).equals("Alice") : "VARCHAR mismatch";

    System.out.println("✓ Mixed types write-read successful");
    System.out.println("  Original: " + original);
    System.out.println("  Read:     " + read + "\n");
  }

  private static void testMultipleRecords() {
    System.out.println("Test 5: Multiple records at different positions");

    Column[] cols = { new Column("id", ColumnType.INT), new Column("name", ColumnType.VARCHAR, 10) };
    Relation rel = new Relation("Items", cols);

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    Record r1 = new Record(new Object[] { 1, "First" });
    Record r2 = new Record(new Object[] { 2, "Second" });
    Record r3 = new Record(new Object[] { 3, "Third" });

    int recordSize = rel.getRecordSize();

    rel.writeRecordToBuffer(r1, buffer, 0);
    rel.writeRecordToBuffer(r2, buffer, recordSize);
    rel.writeRecordToBuffer(r3, buffer, recordSize * 2);

    Record read1 = new Record(2);
    Record read2 = new Record(2);
    Record read3 = new Record(2);

    rel.readFromBuffer(read1, buffer, 0);
    rel.readFromBuffer(read2, buffer, recordSize);
    rel.readFromBuffer(read3, buffer, recordSize * 2);

    assert read1.getValue(0).equals(1) && read1.getValue(1).equals("First");
    assert read2.getValue(0).equals(2) && read2.getValue(1).equals("Second");
    assert read3.getValue(0).equals(3) && read3.getValue(1).equals("Third");

    System.out.println("✓ Multiple records at different positions successful");
    System.out.println("  Record size: " + recordSize + " bytes");
    System.out.println("  Read1: " + read1);
    System.out.println("  Read2: " + read2);
    System.out.println("  Read3: " + read3 + "\n");
  }
}
