package FileManager;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents a database table schema with column definitions.
 * Uses Column[] array for performance.
 * Pre-calculates recordSize for buffer operations.
 */
public class Relation {
  private final String relationName;
  private final Column[] columns;
  private final int recordSize;

  public Relation(String relationName, Column[] columns) {
    this.relationName = relationName;
    this.columns = Arrays.copyOf(columns, columns.length);
    this.recordSize = calculateRecordSize();
  }

  private int calculateRecordSize() {
    int size = 0;
    for (Column col : columns) {
      size += col.getSizeInBytes();
    }
    return size;
  }

  public String getRelationName() {
    return relationName;
  }

  public int getColumnCount() {
    return columns.length;
  }

  public Column[] getColumns() {
    return Arrays.copyOf(columns, columns.length);
  }

  public Column getColumn(int index) {
    if (index < 0 || index >= columns.length) {
      throw new IndexOutOfBoundsException("Column index " + index + " out of bounds");
    }
    return columns[index];
  }

  public int getRecordSize() {
    return recordSize;
  }

  /**
   * Validates that a record matches this relation's schema.
   * Checks arity and type compatibility.
   */
  public void validateRecord(Record record) {
    if (record.getValueCount() != columns.length) {
      throw new IllegalArgumentException(
          "Record has " + record.getValueCount() + " values, expected " + columns.length);
    }

    for (int i = 0; i < columns.length; i++) {
      Object value = record.getValue(i);
      if (value != null && !isValueCompatible(value, columns[i])) {
        throw new IllegalArgumentException(
            "Value at index " + i + " is incompatible with column " + columns[i].getColumnName());
      }
    }
  }

  private boolean isValueCompatible(Object value, Column column) {
    switch (column.getColumnType()) {
      case INT:
        return value instanceof Integer;
      case FLOAT:
        return value instanceof Float || value instanceof Double;
      case CHAR:
      case VARCHAR:
        return value instanceof String && ((String) value).length() <= column.getSizeInBytes();
      default:
        return false;
    }
  }

  /**
   * Writes a record to a ByteBuffer at the specified position.
   * Uses fixed-size format: INT(4), FLOAT(4), CHAR(N), VARCHAR(N).
   * Performance: Direct ByteBuffer operations, no temporary allocations.
   */
  public void writeRecordToBuffer(Record record, ByteBuffer buffer, int pos) {
    int currentPos = pos;

    for (int i = 0; i < columns.length; i++) {
      Column col = columns[i];
      Object value = record.getValue(i);

      buffer.position(currentPos);

      switch (col.getColumnType()) {
        case INT:
          buffer.putInt(value != null ? (Integer) value : 0);
          currentPos += 4;
          break;

        case FLOAT:
          float floatVal = 0.0f;
          if (value != null) {
            floatVal = (value instanceof Float) ? (Float) value : ((Double) value).floatValue();
          }
          buffer.putFloat(floatVal);
          currentPos += 4;
          break;

        case CHAR:
          String charStr = value != null ? (String) value : "";
          int charSize = col.getSizeInBytes();
          for (int j = 0; j < charSize; j++) {
            buffer.put((byte) (j < charStr.length() ? charStr.charAt(j) : ' '));
          }
          currentPos += charSize;
          break;

        case VARCHAR:
          String varcharStr = value != null ? (String) value : "";
          int varcharSize = col.getSizeInBytes();
          for (int j = 0; j < varcharSize; j++) {
            buffer.put((byte) (j < varcharStr.length() ? varcharStr.charAt(j) : ' '));
          }
          currentPos += varcharSize;
          break;
      }
    }
  }

  /**
   * Reads a record from a ByteBuffer at the specified position.
   * Creates properly typed values (Integer, Float, String).
   * Performance: Direct ByteBuffer operations, minimal allocations.
   */
  public void readFromBuffer(Record record, ByteBuffer buffer, int pos) {
    int currentPos = pos;

    for (int i = 0; i < columns.length; i++) {
      Column col = columns[i];
      buffer.position(currentPos);

      switch (col.getColumnType()) {
        case INT:
          record.setValue(i, Integer.valueOf(buffer.getInt()));
          currentPos += 4;
          break;

        case FLOAT:
          record.setValue(i, Float.valueOf(buffer.getFloat()));
          currentPos += 4;
          break;

        case CHAR:
          int charSize = col.getSizeInBytes();
          byte[] charBytes = new byte[charSize];
          buffer.get(charBytes);
          record.setValue(i, new String(charBytes).trim());
          currentPos += charSize;
          break;

        case VARCHAR:
          int varcharSize = col.getSizeInBytes();
          byte[] varcharBytes = new byte[varcharSize];
          buffer.get(varcharBytes);
          record.setValue(i, new String(varcharBytes).trim());
          currentPos += varcharSize;
          break;
      }
    }
  }

  @Override
  public String toString() {
    return "Relation{name='" + relationName + "', columns=" + Arrays.toString(columns) + ", recordSize=" + recordSize
        + "}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    Relation relation = (Relation) obj;
    return relationName.equals(relation.relationName) && Arrays.equals(columns, relation.columns);
  }

  @Override
  public int hashCode() {
    int result = relationName.hashCode();
    result = 31 * result + Arrays.hashCode(columns);
    return result;
  }
}
