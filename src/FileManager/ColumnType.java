package FileManager;

public enum ColumnType {
  INT,
  FLOAT,
  CHAR,
  VARCHAR;

  public static ColumnType fromString(String typeString) {
    String upperType = typeString.toUpperCase();
    if (upperType.equals("INT")) {
      return INT;
    } else if (upperType.equals("FLOAT")) {
      return FLOAT;
    } else if (upperType.startsWith("CHAR(")) {
      return CHAR;
    } else if (upperType.startsWith("VARCHAR(")) {
      return VARCHAR;
    }
    throw new IllegalArgumentException("Type non reconnu: " + typeString);
  }

  public int getFixedSize() {
    switch (this) {
      case INT:
      case FLOAT:
        return 4;
      case CHAR:
      case VARCHAR:
        return -1;
      default:
        throw new IllegalStateException("Type inconnu: " + this);
    }
  }
}