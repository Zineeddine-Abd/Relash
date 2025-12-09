package QueryManager;

import FileManager.Record;
import java.util.List;

public class SelectOperator implements IRecordIterator {
    private IRecordIterator child;
    private List<Condition> conditions;

    public SelectOperator(IRecordIterator child, List<Condition> conditions) {
        this.child = child;
        this.conditions = conditions;
    }

    @Override
    public Record GetNextRecord() {
        Record rec;
        while ((rec = child.GetNextRecord()) != null) {
            boolean valid = true;
            if (conditions != null) {
                for (Condition cond : conditions) {
                    if (!cond.evaluate(rec)) {
                        valid = false;
                        break;
                    }
                }
            }
            if (valid) return rec;
        }
        return null;
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