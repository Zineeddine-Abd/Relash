package FileManager;

import java.util.Arrays;

public class Record {
    private Object[] values;
    private RecordId rid; // Ajout TP7 : Stocker le RID du record

    public Record(int capacity) {
        this.values = new Object[capacity];
    }

    public Record(Object[] values) {
        this.values = Arrays.copyOf(values, values.length);
    }

    // --- Ajout TP7 ---
    public void setRid(RecordId rid) {
        this.rid = rid;
    }

    public RecordId getRid() {
        return rid;
    }
    // ----------------

    public Object getValue(int index) {
        if (index < 0 || index >= values.length) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds");
        }
        return values[index];
    }

    public void setValue(int index, Object value) {
        if (index < 0 || index >= values.length) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds");
        }
        values[index] = value;
    }

    public int getValueCount() {
        return values.length;
    }

    @Override
    public String toString() {
        // Format demandé TP7 : val1 ; val2 ; ... .
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            sb.append(values[i]);
            if (i < values.length - 1) sb.append(" ; ");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        return Arrays.equals(values, ((Record) obj).values);
    }
}