package FileManager;

import BufferManager.BufferManager;
import DiskManager.DiskManager;
import DiskManager.PageId;
import Main.DBConfig;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Relation {

    private final String relationName;
    private final Column[] columns;
    private final int recordSize;

    private final DiskManager diskManager;
    private final BufferManager bufferManager;
    private final PageId headerPageId;
    private final int slotCountPerDataPage;

    // Constantes
    private static final PageId DUMMY_PAGE_ID = new PageId(-1, -1);
    private static final int NEXT_PAGE_ID_OFFSET = 0;
    private static final int PREV_PAGE_ID_OFFSET = 8;
    private static final int DATA_PAGE_HEADER_SIZE = 16;
    private final int BYTEMAP_OFFSET = DATA_PAGE_HEADER_SIZE;
    private final int RECORDS_OFFSET;
    private static final int FREE_LIST_HEAD_OFFSET = 0;
    private static final int FULL_LIST_HEAD_OFFSET = 8;

    public Relation(String relationName, Column[] columns, DBConfig config,
                    DiskManager diskManager, BufferManager bufferManager, PageId headerPageId) {
        this.relationName = relationName;
        this.columns = Arrays.copyOf(columns, columns.length);
        this.recordSize = calculateRecordSize();
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.headerPageId = headerPageId;

        int usableSpace = config.getPageSize() - DATA_PAGE_HEADER_SIZE;
        this.slotCountPerDataPage = usableSpace / (1 + this.recordSize);
        this.RECORDS_OFFSET = BYTEMAP_OFFSET + this.slotCountPerDataPage;
    }

    private int calculateRecordSize() {
        int size = 0;
        for (Column col : columns) {
            size += col.getSizeInBytes();
        }
        return size;
    }

    // Met à jour un record existant (sans changer son RID)
    public void updateRecord(RecordId rid, Record newRecord) {
        PageId pageId = rid.getPageId();
        int slotIdx = rid.getSlotIdx();

        ByteBuffer pageBuff = bufferManager.GetPage(pageId);

        // Calculer la position et écrire
        int pos = RECORDS_OFFSET + (slotIdx * recordSize);
        writeRecordToBuffer(newRecord, pageBuff, pos);

        bufferManager.FreePage(pageId, true); // Dirty = true
    }

    // Helper pour récupérer l'index d'une colonne par son nom
    public int getColumnIndex(String colName) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getColumnName().equals(colName)) {
                return i;
            }
        }
        return -1;
    }

    public String getRelationName() { return relationName; }
    public Column[] getColumns() { return Arrays.copyOf(columns, columns.length); }
    public int getRecordSize() { return recordSize; }
    public PageId getHeaderPageId() { return headerPageId; }
    public int getSlotCountPerDataPage() { return slotCountPerDataPage; }

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
                case VARCHAR:
                    String str = value != null ? (String) value : "";
                    int size = col.getSizeInBytes();
                    for (int j = 0; j < size; j++) {
                        buffer.put((byte) (j < str.length() ? str.charAt(j) : ' ')); // Padding simple
                    }
                    currentPos += size;
                    break;
            }
        }
    }

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
                case VARCHAR:
                    int size = col.getSizeInBytes();
                    byte[] bytes = new byte[size];
                    buffer.get(bytes);
                    record.setValue(i, new String(bytes).trim()); // Trim padding
                    currentPos += size;
                    break;
            }
        }
    }

    public RecordId InsertRecord(Record record) {
        PageId pageIdToInsert = getFreeDataPageId();
        if (pageIdToInsert.equals(DUMMY_PAGE_ID)) {
            pageIdToInsert = addDataPage();
        }
        RecordId rid = writeRecordToDataPage(record, pageIdToInsert);
        return rid;
    }

    public List<Record> GetAllRecords() {
        List<Record> allRecords = new ArrayList<>();
        List<PageId> allPages = getDataPages();
        for (PageId pageId : allPages) {
            allRecords.addAll(getRecordsInDataPage(pageId));
        }
        return allRecords;
    }

    public void DeleteRecord(RecordId rid) {
        PageId pageId = rid.getPageId();
        int slotIdx = rid.getSlotIdx();
        ByteBuffer pageBuff = bufferManager.GetPage(pageId);

        boolean wasFull = true;
        for (int i = 0; i < slotCountPerDataPage; i++) {
            if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 0) {
                wasFull = false;
                break;
            }
        }

        pageBuff.put(BYTEMAP_OFFSET + slotIdx, (byte) 0);

        if (wasFull) {
            movePageToFreeList(pageId);
        }

        boolean pageEmpty = true;
        for (int i = 0; i < slotCountPerDataPage; i++) {
            if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 1) {
                pageEmpty = false;
                break;
            }
        }

        if (pageEmpty) {
            unlinkPage(pageId, FREE_LIST_HEAD_OFFSET);
            diskManager.DeallocPage(pageId);
        }

        bufferManager.FreePage(pageId, true);
    }

    private PageId addDataPage() {
        PageId newDataPageId = diskManager.AllocPage();
        ByteBuffer newPageBuff = bufferManager.GetPage(newDataPageId);
        for (int i = 0; i < slotCountPerDataPage; i++) {
            newPageBuff.put(BYTEMAP_OFFSET + i, (byte) 0);
        }
        linkPageToHead(newDataPageId, FREE_LIST_HEAD_OFFSET);
        bufferManager.FreePage(newDataPageId, true);
        return newDataPageId;
    }

    private PageId getFreeDataPageId() {
        ByteBuffer headerBuff = bufferManager.GetPage(headerPageId);
        PageId freeHeadId = readPageIdFromBuffer(headerBuff, FREE_LIST_HEAD_OFFSET);
        bufferManager.FreePage(headerPageId, false);
        return freeHeadId;
    }

    private RecordId writeRecordToDataPage(Record record, PageId pageIdToInsert) {
        ByteBuffer pageBuff = bufferManager.GetPage(pageIdToInsert);
        int slotIdx = -1;
        for (int i = 0; i < slotCountPerDataPage; i++) {
            if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 0) {
                slotIdx = i;
                break;
            }
        }

        if (slotIdx == -1) throw new RuntimeException("Erreur: page pleine.");

        pageBuff.put(BYTEMAP_OFFSET + slotIdx, (byte) 1);
        int pos = RECORDS_OFFSET + (slotIdx * recordSize);
        writeRecordToBuffer(record, pageBuff, pos);

        boolean pageFull = true;
        for (int i = 0; i < slotCountPerDataPage; i++) {
            if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 0) {
                pageFull = false;
                break;
            }
        }

        if (pageFull) {
            movePageToFullList(pageIdToInsert);
        }

        bufferManager.FreePage(pageIdToInsert, true);
        return new RecordId(pageIdToInsert, slotIdx);
    }

    public List<Record> getRecordsInDataPage(PageId pageId) {
        List<Record> records = new ArrayList<>();
        ByteBuffer pageBuff = bufferManager.GetPage(pageId);

        for (int i = 0; i < slotCountPerDataPage; i++) {
            if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 1) {
                int pos = RECORDS_OFFSET + (i * recordSize);
                Record rec = new Record(columns.length);
                readFromBuffer(rec, pageBuff, pos);
                rec.setRid(new RecordId(pageId, i));
                records.add(rec);
            }
        }

        bufferManager.FreePage(pageId, false);
        return records;
    }

    public List<PageId> getDataPages() {
        List<PageId> allPages = new ArrayList<>();
        ByteBuffer headerBuff = bufferManager.GetPage(headerPageId);
        PageId freeHead = readPageIdFromBuffer(headerBuff, FREE_LIST_HEAD_OFFSET);
        PageId fullHead = readPageIdFromBuffer(headerBuff, FULL_LIST_HEAD_OFFSET);
        bufferManager.FreePage(headerPageId, false);

        traverseList(freeHead, allPages);
        traverseList(fullHead, allPages);
        return allPages;
    }

    public int getRecordsOffset() {
        return RECORDS_OFFSET;
    }

    private void traverseList(PageId head, List<PageId> results) {
        PageId currentId = head;
        while (!currentId.equals(DUMMY_PAGE_ID)) {
            results.add(currentId);
            ByteBuffer currentBuff = bufferManager.GetPage(currentId);

            // Lire l'ID de la prochaine page et le stocker temporairement
            PageId nextId = readPageIdFromBuffer(currentBuff, NEXT_PAGE_ID_OFFSET);

            // Libérer la page courante (celle qu'on a actuellement)
            bufferManager.FreePage(currentId, false);

            // Mettre à jour currentId pour la prochaine iteration de la boucle
            currentId = nextId;
        }
    }

    private void linkPageToHead(PageId pageId, int headerOffset) {
        ByteBuffer headerBuff = bufferManager.GetPage(headerPageId);
        PageId oldHeadId = readPageIdFromBuffer(headerBuff, headerOffset);

        writePageIdToBuffer(pageId, headerBuff, headerOffset);
        bufferManager.FreePage(headerPageId, true);

        ByteBuffer pageBuff = bufferManager.GetPage(pageId);
        writePageIdToBuffer(oldHeadId, pageBuff, NEXT_PAGE_ID_OFFSET);
        writePageIdToBuffer(DUMMY_PAGE_ID, pageBuff, PREV_PAGE_ID_OFFSET);
        bufferManager.FreePage(pageId, true);

        if (!oldHeadId.equals(DUMMY_PAGE_ID)) {
            ByteBuffer oldHeadBuff = bufferManager.GetPage(oldHeadId);
            writePageIdToBuffer(pageId, oldHeadBuff, PREV_PAGE_ID_OFFSET);
            bufferManager.FreePage(oldHeadId, true);
        }
    }

    private void unlinkPage(PageId pageId, int headerOffset) {
        ByteBuffer pageBuff = bufferManager.GetPage(pageId);
        PageId nextId = readPageIdFromBuffer(pageBuff, NEXT_PAGE_ID_OFFSET);
        PageId prevId = readPageIdFromBuffer(pageBuff, PREV_PAGE_ID_OFFSET);
        bufferManager.FreePage(pageId, false);

        if (!nextId.equals(DUMMY_PAGE_ID)) {
            ByteBuffer nextBuff = bufferManager.GetPage(nextId);
            writePageIdToBuffer(prevId, nextBuff, PREV_PAGE_ID_OFFSET);
            bufferManager.FreePage(nextId, true);
        }

        if (!prevId.equals(DUMMY_PAGE_ID)) {
            ByteBuffer prevBuff = bufferManager.GetPage(prevId);
            writePageIdToBuffer(nextId, prevBuff, NEXT_PAGE_ID_OFFSET);
            bufferManager.FreePage(prevId, true);
        } else {
            ByteBuffer headerBuff = bufferManager.GetPage(headerPageId);
            writePageIdToBuffer(nextId, headerBuff, headerOffset);
            bufferManager.FreePage(headerPageId, true);
        }
    }

    private void movePageToFullList(PageId pageId) {
        unlinkPage(pageId, FREE_LIST_HEAD_OFFSET);
        linkPageToHead(pageId, FULL_LIST_HEAD_OFFSET);
    }

    private void movePageToFreeList(PageId pageId) {
        unlinkPage(pageId, FULL_LIST_HEAD_OFFSET);
        linkPageToHead(pageId, FREE_LIST_HEAD_OFFSET);
    }

    private PageId readPageIdFromBuffer(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        int fileIdx = buffer.getInt();
        int pageIdx = buffer.getInt();
        return new PageId(fileIdx, pageIdx);
    }

    private void writePageIdToBuffer(PageId pageId, ByteBuffer buffer, int offset) {
        buffer.position(offset);
        buffer.putInt(pageId.getFileIdx());
        buffer.putInt(pageId.getPageIdx());
    }
}