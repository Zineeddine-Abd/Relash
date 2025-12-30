package QueryManager;

import BufferManager.BufferManager;
import DiskManager.PageId;
import FileManager.Record;
import FileManager.Relation;
import FileManager.RecordId;

import java.nio.ByteBuffer;
import java.util.List;

public class RelationScanner implements IRecordIterator {
    private Relation relation;
    private BufferManager bm;
    private List<PageId> dataPages;
    private int currentPageIdx;
    private int currentSlotIdx;

    public RelationScanner(Relation relation, BufferManager bm) {
        this.relation = relation;
        this.bm = bm;
        Reset();
    }

    @Override
    public void Reset() {
        this.dataPages = relation.getDataPages();
        this.currentPageIdx = 0;
        this.currentSlotIdx = 0;
    }

    @Override
    public void Close() {
        // Rien
    }

    @Override
    public Record GetNextRecord() {
        if (dataPages.isEmpty()) return null;

        while (currentPageIdx < dataPages.size()) {
            PageId pid = dataPages.get(currentPageIdx);
            ByteBuffer buff = bm.GetPage(pid);

            // Chercher le prochain slot valide dans la page courante
            int slotsPerPage = relation.getSlotCountPerDataPage();

            // ByteMap offset = 16 
            int bytemapOffset = 16;

            // Parcourir les slots restants de la page
            while (currentSlotIdx < slotsPerPage) {
                byte occupied = buff.get(bytemapOffset + currentSlotIdx);

                if (occupied == 1) {
                    // Record trouvé
                    // Calculer position (16 + slots + idx * recordSize)
                    // Note: Relation has a helper for this offset calculation but it's private.
                    // Recomputing based on Relation logic:
                    // Header(16) + Bytemap(slots)
                    int recordsStartOffset = 16 + slotsPerPage;
                    int pos = recordsStartOffset + (currentSlotIdx * relation.getRecordSize());

                    Record rec = new Record(relation.getColumns().length);
                    relation.readFromBuffer(rec, buff, pos);
                    rec.setRid(new RecordId(pid, currentSlotIdx));

                    bm.FreePage(pid, false);
                    currentSlotIdx++;
                    return rec;
                }
                currentSlotIdx++;
            }

            // Fin de la page atteinte sans trouver (ou plus de records), passer a la suivante
            bm.FreePage(pid, false);
            currentPageIdx++;
            currentSlotIdx = 0;
        }

        return null; // Plus de records
    }
}