package QueryManager;

import FileManager.Record;
import java.util.List;

public class ProjectOperator implements IRecordIterator {
    private IRecordIterator child;
    private List<Integer> colIndices; // Liste des indices à garder

    public ProjectOperator(IRecordIterator child, List<Integer> colIndices) {
        this.child = child;
        this.colIndices = colIndices;
    }

    @Override
    public Record GetNextRecord() {
        Record sourceRec = child.GetNextRecord();
        if (sourceRec == null) return null;

        // Si colIndices est null ou vide -> SELECT *
        if (colIndices == null || colIndices.isEmpty()) {
            return sourceRec;
        }

        Record newRec = new Record(colIndices.size());
        for (int i = 0; i < colIndices.size(); i++) {
            newRec.setValue(i, sourceRec.getValue(colIndices.get(i)));
        }
        return newRec;
    }

    @Override
    public void Close() {
        child.Close();
    }

    @Override
    public void Reset() {
        child.Reset();
    }
}