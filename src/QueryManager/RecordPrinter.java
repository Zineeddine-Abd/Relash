package QueryManager;

import FileManager.Record;

public class RecordPrinter {
    private IRecordIterator iterator;

    public RecordPrinter(IRecordIterator iterator) {
        this.iterator = iterator;
    }

    public void print() {
        int count = 0;
        Record rec;
        while ((rec = iterator.GetNextRecord()) != null) {
            System.out.println(rec.toString() + ".");
            count++;
        }
        System.out.println("Total selected records = " + count);
    }
}