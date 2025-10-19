package BufferManager;

import DiskManager.PageId;
import java.nio.ByteBuffer;


 // Represents a single buffer frame in the buffer pool.

public class Buffer {
    private PageId pageId;
    private ByteBuffer data;
    private int pin_count;
    private boolean dirty;
    private long lastAccessTime;
    private boolean valid;

    public Buffer(int pageSize) {
        this.data = ByteBuffer.allocate(pageSize);
        this.pageId = null;
        this.pin_count = 0;
        this.dirty = false;
        this.lastAccessTime = 0;
        this.valid = false;
    }

    // Getters and Setters
    public PageId getPageId() { return pageId; }
    public void setPageId(PageId pageId) { this.pageId = pageId; }

    public ByteBuffer getData() { return data; }

    public int getPin_count() { return pin_count; }
    public void incrementPin_count() { this.pin_count++; }
    public void decrementPin_count() {
        if (this.pin_count > 0) {
            this.pin_count--;
        }
    }

    public boolean isDirty() { return dirty; }
    public void setDirty(boolean dirty) { this.dirty = dirty; }

    public long getLastAccessTime() { return lastAccessTime; }
    public void setLastAccessTime(long lastAccessTime) { this.lastAccessTime = lastAccessTime; }

    public boolean isValid() { return valid; }
    public void setValid(boolean valid) { this.valid = valid; }

    public boolean isPinned() { return pin_count > 0; }

    public void reset() {
        this.pageId = null;
        this.pin_count = 0;
        this.dirty = false;
        this.lastAccessTime = 0;
        this.valid = false;
        this.data.clear();
    }
}