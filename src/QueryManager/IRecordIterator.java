package QueryManager;

import FileManager.Record;

public interface IRecordIterator {
    Record GetNextRecord();
    void Close();
    void Reset();
}