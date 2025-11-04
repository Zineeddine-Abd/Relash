package FileManager;

import java.util.Arrays;

public class Record {
  private Object[] values;

  public Record(int capacity) {
    this.values = new Object[capacity];
  }

  public Record(Object[] values) {
    this.values = Arrays.copyOf(values, values.length);
  }

  public Object getValue(int index) {
    if (index < 0 || index >= values.length) {
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for " + values.length + " values");
    }
    return values[index];
  }

  public void setValue(int index, Object value) {
    if (index < 0 || index >= values.length) {
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for " + values.length + " values");
    }
    values[index] = value;
  }

  public int getValueCount() {
    return values.length;
  }

  @Override
  public String toString() {
    return "Record" + Arrays.toString(values);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    return Arrays.equals(values, ((Record) obj).values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }
}
