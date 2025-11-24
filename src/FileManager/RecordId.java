package FileManager;

import DiskManager.PageId;

public class RecordId {
    private final PageId pageId;
    private final int slotIdx;

    public RecordId(PageId pageId, int slotIdx) {
        this.pageId = pageId;
        this.slotIdx = slotIdx;
    }

    public PageId getPageId() {
        return pageId;
    }

    public int getSlotIdx() {
        return slotIdx;
    }

    @Override
    public String toString() {
        return "RecordId(PageId=" + pageId.toString() + ", SlotIdx=" + slotIdx + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        RecordId recordId = (RecordId) obj;
        return slotIdx == recordId.slotIdx && pageId.equals(recordId.pageId);
    }
}