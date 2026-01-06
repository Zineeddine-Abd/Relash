package QueryManager;

import FileManager.ColumnType;
import FileManager.Record;

public class Condition {

    private int colIndex;
    private String operator;
    private String valConst; // Stocké en String, converti à la volée
    private int colIndexRight; // Si comparaison col vs col, sinon -1
    private boolean isRightConstant;
    private ColumnType type; // Type de la colonne de gauche (juste pour le casting)

    public Condition(int colIndex, String operator, String valConst, ColumnType type) {
        this.colIndex = colIndex;
        this.operator = operator;
        this.valConst = valConst;
        this.isRightConstant = true;
        this.type = type;
        this.colIndexRight = -1;
    }

    public Condition(int colIndex, String operator, int colIndexRight, ColumnType type) {
        this.colIndex = colIndex;
        this.operator = operator;
        this.colIndexRight = colIndexRight;
        this.isRightConstant = false;
        this.type = type;
        this.valConst = null;
    }

    public boolean evaluate(Record record) {
        Object v1 = record.getValue(colIndex);
        Object v2;

        if (isRightConstant) {
            v2 = parseConstant(valConst, type);
        } else {
            v2 = record.getValue(colIndexRight);
        }

        // Comparaison
        if (type == ColumnType.INT) {
            int i1 = (Integer) v1;
            int i2 = (Integer) v2;
            return compareInt(i1, i2);
        } else if (type == ColumnType.FLOAT) {
            float f1 = (Float) v1;
            float f2 = (Float) v2;
            return compareFloat(f1, f2);
        } else {
            String s1 = (String) v1;
            String s2 = (String) v2;
            return compareString(s1, s2);
        }
    }

    private Object parseConstant(String val, ColumnType t) {
        try {
            switch (t) {
                case INT: return Integer.parseInt(val);
                case FLOAT: return Float.parseFloat(val);
                default: return val.replace("\"", ""); // Enlever les guillemets
            }
        } catch (Exception e) {
            throw new RuntimeException("Erreur de type dans la condition WHERE: " + val + " n'est pas " + t);
        }
    }

    private boolean compareInt(int a, int b) {
        switch (operator) {
            case "=": return a == b;
            case "<": return a < b;
            case ">": return a > b;
            case "<=": return a <= b;
            case ">=": return a >= b;
            case "<>": return a != b;
            default: return false;
        }
    }

    private boolean compareFloat(float a, float b) {
        switch (operator) {
            case "=": return Math.abs(a - b) < 0.0001;
            case "<": return a < b;
            case ">": return a > b;
            case "<=": return a <= b;
            case ">=": return a >= b;
            case "<>": return Math.abs(a - b) > 0.0001;
            default: return false;
        }
    }

    private boolean compareString(String a, String b) {
        int res = a.compareTo(b);
        switch (operator) {
            case "=": return res == 0;
            case "<": return res < 0;
            case ">": return res > 0;
            case "<=": return res <= 0;
            case ">=": return res >= 0;
            case "<>": return res != 0;
            default: return false;
        }
    }
}