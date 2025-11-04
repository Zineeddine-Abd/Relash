package FileManager;

public class Column {
  private final String columnName;
  private final ColumnType columnType;
  private final int sizeInBytes;

  public Column(String columnName, ColumnType columnType, int sizeParameter) {
    this.columnName = columnName;
    this.columnType = columnType;
    this.sizeInBytes = calculateSize(columnType, sizeParameter);
  }

  public Column(String columnName, ColumnType columnType) {
    this(columnName, columnType, 0);
  }

  public Column(String columnName, String typeString) {
    this.columnName = columnName;
    this.columnType = ColumnType.fromString(typeString);
    this.sizeInBytes = calculateSize(ColumnType.fromString(typeString), extractSizeParameter(typeString));
  }

  private int extractSizeParameter(String typeString) {
    if (typeString.startsWith("CHAR(") || typeString.startsWith("VARCHAR(")) {
      int startIdx = typeString.indexOf('(') + 1;
      int endIdx = typeString.indexOf(')');
      return Integer.parseInt(typeString.substring(startIdx, endIdx));
    }
    return 0;
  }

  private int calculateSize(ColumnType columnType, int sizeParameter) {
    switch (columnType) {
      case INT:
      case FLOAT:
        return 4;
      case CHAR:
      case VARCHAR:
        return sizeParameter;
      default:
        throw new IllegalArgumentException("Type de colonne non valide: " + columnType
            + " Utiliser un type valide parmis INT, FLOAT, CHAR(T), VARCHAR(T).");
    }
  }

  public String getColumnName() {
    return columnName;
  }

  public ColumnType getColumnType() {
    return columnType;
  }

  public int getSizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "Column{columnName='" + columnName + "', columnType=" + columnType + ", sizeInBytes=" + sizeInBytes
        + "}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    Column column = (Column) obj;
    return sizeInBytes == column.sizeInBytes &&
        columnName.equals(column.columnName) &&
        columnType == column.columnType;
  }

  @Override
  public int hashCode() {
    int result = columnName.hashCode();
    result = 31 * result + columnType.hashCode();
    result = 31 * result + sizeInBytes;
    return result;
  }

}
