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

  // Constantes pour la gestion des pages ---
  private static final PageId DUMMY_PAGE_ID = new PageId(-1, -1);

  // Data Page Offsets ---
  // Offset des pointeurs de la liste doublement chainee dans une DataPage
  private static final int NEXT_PAGE_ID_OFFSET = 0; // 8 octets
  private static final int PREV_PAGE_ID_OFFSET = 8; // 8 octets
  private static final int DATA_PAGE_HEADER_SIZE = 16; // 8 + 8
  // Offset de la bytemap dans une DataPage
  private final int BYTEMAP_OFFSET = DATA_PAGE_HEADER_SIZE;
  // Offset du debut des records dans une DataPage
  private final int RECORDS_OFFSET;

  // Header Page Offsets ---
  // Offsets dans la Header Page
  private static final int FREE_LIST_HEAD_OFFSET = 0; // 8 octets
  private static final int FULL_LIST_HEAD_OFFSET = 8; // 8 octets

  public Relation(String relationName, Column[] columns, DBConfig config,
      DiskManager diskManager, BufferManager bufferManager, PageId headerPageId) {
    this.relationName = relationName;
    this.columns = Arrays.copyOf(columns, columns.length);
    this.recordSize = calculateRecordSize();

    this.diskManager = diskManager;
    this.bufferManager = bufferManager;
    this.headerPageId = headerPageId;

    // Calcul du nombre de slots (cases) par page
    int usableSpace = config.getPageSize() - DATA_PAGE_HEADER_SIZE;
    // 1 octet pour la bytemap + la taille du record
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

  public String getRelationName() {
    return relationName;
  }

  public Column[] getColumns() {
    return Arrays.copyOf(columns, columns.length);
  }

  public int getRecordSize() {
    return recordSize;
  }

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
            buffer.put((byte) (j < str.length() ? str.charAt(j) : ' '));
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
          record.setValue(i, new String(bytes).trim());
          currentPos += size;
          break;
      }
    }
  }

  public RecordId InsertRecord(Record record) {
    PageId pageIdToInsert = getFreeDataPageId();

    if (pageIdToInsert.equals(DUMMY_PAGE_ID)) {
      // Aucune page libre n'existe, il faut en creer une.
      pageIdToInsert = addDataPage();
    }

    // Ecrire le record et obtenir son slot
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

    // Verifier si la page etait pleine AVANT la suppression
    boolean wasFull = true;
    for (int i = 0; i < slotCountPerDataPage; i++) {
      if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 0) {
        wasFull = false;
        break;
      }
    }

    // Marquer le slot comme libre (0) dans la bytemap
    pageBuff.put(BYTEMAP_OFFSET + slotIdx, (byte) 0);

    // Si la page ETAIT pleine et ne l'est PLUS, la deplacer
    if (wasFull) {
      movePageToFreeList(pageId);
    }

    // Vérifier si la page est maintenant vide
    boolean pageEmpty = true;
    for (int i = 0; i < slotCountPerDataPage; i++) {
      if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 1) {
        pageEmpty = false;
        break;
      }
    }

    if (pageEmpty) {
      // Sortir la page de la liste (elle est dans la free list)
      unlinkPage(pageId, FREE_LIST_HEAD_OFFSET);
      // Liberer la page sur le disque
      diskManager.DeallocPage(pageId);
    }

    bufferManager.FreePage(pageId, true); // Page modifiee
  }

  private PageId addDataPage() {
    PageId newDataPageId = diskManager.AllocPage();
    ByteBuffer newPageBuff = bufferManager.GetPage(newDataPageId);

    // Initialiser la page : bytemap a 0
    for (int i = 0; i < slotCountPerDataPage; i++) {
      newPageBuff.put(BYTEMAP_OFFSET + i, (byte) 0);
    }

    // L'ajouter en tete de la liste (premiere position)s des pages libres
    linkPageToHead(newDataPageId, FREE_LIST_HEAD_OFFSET);

    bufferManager.FreePage(newDataPageId, true); // Page modifiee
    return newDataPageId;
  }

  // le parametre sizeRecord est inutile parceque tous les records ont la meme
  // taille
  private PageId getFreeDataPageId() {
    ByteBuffer headerBuff = bufferManager.GetPage(headerPageId);
    PageId freeHeadId = readPageIdFromBuffer(headerBuff, FREE_LIST_HEAD_OFFSET);
    bufferManager.FreePage(headerPageId, false);
    return freeHeadId; // Retourne DUMMY_PAGE_ID si la liste est vide
  }

  // utilisant cette signature ca sra plus optimise
  private RecordId writeRecordToDataPage(Record record, PageId pageIdToInsert) {

    ByteBuffer pageBuff = bufferManager.GetPage(pageIdToInsert);

    int slotIdx = -1;
    // Trouver le premier slot libre (0) dans la bytemap
    for (int i = 0; i < slotCountPerDataPage; i++) {
      if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 0) {
        slotIdx = i;
        break;
      }
    }

    if (slotIdx == -1) {
      // Ne devrait pas arriver normalement si getFreeDataPageId() est correct
      throw new RuntimeException("Tentative d'écriture sur une page pleine.");
    }

    // Marquer le slot comme utilisé (1)
    pageBuff.put(BYTEMAP_OFFSET + slotIdx, (byte) 1);

    // Calculer la position d'écriture du record
    int pos = RECORDS_OFFSET + (slotIdx * recordSize);

    // Ecrire le record en utilisant la méthode du TP4
    writeRecordToBuffer(record, pageBuff, pos);

    // Vérifier si la page est maintenant pleine
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

    bufferManager.FreePage(pageIdToInsert, true); // Page modifiee

    RecordId rid = new RecordId(pageIdToInsert, slotIdx);

    return rid;
  }

  private List<Record> getRecordsInDataPage(PageId pageId) {
    List<Record> records = new ArrayList<>();
    ByteBuffer pageBuff = bufferManager.GetPage(pageId);

    for (int i = 0; i < slotCountPerDataPage; i++) {
      // Si le slot est utilise (1)
      if (pageBuff.get(BYTEMAP_OFFSET + i) == (byte) 1) {
        int pos = RECORDS_OFFSET + (i * recordSize);
        Record rec = new Record(columns.length);
        readFromBuffer(rec, pageBuff, pos);
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

  // Methodes utilitaires (Listes Chaînées, I/O PageId) ---
  private void traverseList(PageId head, List<PageId> results) {
    PageId currentId = head;
    while (!currentId.equals(DUMMY_PAGE_ID)) {
      results.add(currentId);
      ByteBuffer currentBuff = bufferManager.GetPage(currentId);
      currentId = readPageIdFromBuffer(currentBuff, NEXT_PAGE_ID_OFFSET);
      bufferManager.FreePage(currentId, false);
    }
  }

  // Lie une page en tête de la liste (dont la tête est à headerOffset).
  private void linkPageToHead(PageId pageId, int headerOffset) {
    ByteBuffer headerBuff = bufferManager.GetPage(headerPageId);
    PageId oldHeadId = readPageIdFromBuffer(headerBuff, headerOffset);

    // Mettre à jour la tete de liste dans le header
    writePageIdToBuffer(pageId, headerBuff, headerOffset);
    bufferManager.FreePage(headerPageId, true);

    // Mettre à jour les pointeurs de la nouvelle page
    ByteBuffer pageBuff = bufferManager.GetPage(pageId);
    writePageIdToBuffer(oldHeadId, pageBuff, NEXT_PAGE_ID_OFFSET); // Next = ancienne tete
    writePageIdToBuffer(DUMMY_PAGE_ID, pageBuff, PREV_PAGE_ID_OFFSET); // Prev = null
    bufferManager.FreePage(pageId, true);

    // Mettre à jour le pointeur "prev" de l'ancienne tete (si elle existait)
    if (!oldHeadId.equals(DUMMY_PAGE_ID)) {
      ByteBuffer oldHeadBuff = bufferManager.GetPage(oldHeadId);
      writePageIdToBuffer(pageId, oldHeadBuff, PREV_PAGE_ID_OFFSET);
      bufferManager.FreePage(oldHeadId, true);
    }
  }

  // Sort une page de la liste dont la tete est à headerOffset.
  private void unlinkPage(PageId pageId, int headerOffset) {
    ByteBuffer pageBuff = bufferManager.GetPage(pageId);
    PageId nextId = readPageIdFromBuffer(pageBuff, NEXT_PAGE_ID_OFFSET);
    PageId prevId = readPageIdFromBuffer(pageBuff, PREV_PAGE_ID_OFFSET);
    bufferManager.FreePage(pageId, false);

    // Si le noeud suivant existe, mettre à jour son "prev"
    if (!nextId.equals(DUMMY_PAGE_ID)) {
      ByteBuffer nextBuff = bufferManager.GetPage(nextId);
      writePageIdToBuffer(prevId, nextBuff, PREV_PAGE_ID_OFFSET);
      bufferManager.FreePage(nextId, true);
    }

    // Si le noeud precedent existe, mettre à jour son "next"
    if (!prevId.equals(DUMMY_PAGE_ID)) {
      ByteBuffer prevBuff = bufferManager.GetPage(prevId);
      writePageIdToBuffer(nextId, prevBuff, NEXT_PAGE_ID_OFFSET);
      bufferManager.FreePage(prevId, true);
    } else {
      // Le noeud était la tete de liste, mettre à jour le header
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

  // Helpers I/O
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

  public PageId getHeaderPageId() {
      return headerPageId;
  }

  @Override
  public String toString() {
    return "Relation{name='" + relationName + "', columns=" + Arrays.toString(columns) + ", recordSize=" + recordSize
        + "}";
  }
}