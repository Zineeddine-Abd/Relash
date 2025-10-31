package BufferManager;

import Main.DBConfig;
import DiskManager.DiskManager;
import DiskManager.PageId;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class BufferManager {
    private DBConfig config;
    private DiskManager diskManager;
    private Buffer[] bufferPool;
    private Map<PageId, Integer> pageToBufferMap; // Maps PageId to buffer index
    private String currentPolicy;
    private long currentTime; // For LRU/MRU timestamp


    public BufferManager(DBConfig config, DiskManager diskManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.currentPolicy = config.getBm_policy();
        this.currentTime = 0;

        this.bufferPool = new Buffer[config.getBm_buffercount()];
        for (int i = 0; i < bufferPool.length; i++) {
            bufferPool[i] = new Buffer(config.getPageSize());
        }

        this.pageToBufferMap = new ConcurrentHashMap<>();
    }

    public ByteBuffer GetPage(PageId pageId) {
        synchronized (this) {
            currentTime++;

            // Check if page is already in buffer pool
            Integer bufferIndex = pageToBufferMap.get(pageId);
            if (bufferIndex != null) {
                Buffer buffer = bufferPool[bufferIndex];
                buffer.incrementPin_count();
                buffer.setLastAccessTime(currentTime);
                return buffer.getData();
            }

            // Page not in buffer pool, need to load it
            int targetBufferIndex = findReplacementBuffer();
            Buffer targetBuffer = bufferPool[targetBufferIndex];

            // If target buffer has a dirty page, write it to disk
            if (targetBuffer.isValid() && targetBuffer.isDirty()) {
                diskManager.WritePage(targetBuffer.getPageId(), targetBuffer.getData());
                targetBuffer.setDirty(false);
            }

            // Remove old mapping if buffer was in use
            if (targetBuffer.isValid() && targetBuffer.getPageId() != null) {
                pageToBufferMap.remove(targetBuffer.getPageId());
            }

            // Load new page from disk
            targetBuffer.reset();
            diskManager.ReadPage(pageId, targetBuffer.getData());
            targetBuffer.setPageId(pageId);
            targetBuffer.setValid(true);
            targetBuffer.incrementPin_count();
            targetBuffer.setLastAccessTime(currentTime);

            // Update the mapping
            pageToBufferMap.put(pageId, targetBufferIndex);

            return targetBuffer.getData();
        }
    }

    public void FreePage(PageId pageId, boolean valdirty) {
        synchronized (this) {
            Integer bufferIndex = pageToBufferMap.get(pageId);
            if (bufferIndex != null) {
                Buffer buffer = bufferPool[bufferIndex];
                buffer.decrementPin_count();
                if (valdirty) {
                    buffer.setDirty(true);
                }
            }
        }
    }

    public void SetCurrentReplacementPolicy(String policy) {
        synchronized (this) {
            if (policy.equals("LRU") || policy.equals("MRU")) {
                this.currentPolicy = policy;
            } else {
                throw new IllegalArgumentException("Invalid policy: " + policy +
                        "Que LRU et MRU are Sont valides");
            }
        }
    }

    // Flushes all dirty pages to disk and resets all buffers.
    public void FlushBuffers() {
        synchronized (this) {
            // Write all dirty pages to disk
            for (Buffer buffer : bufferPool) {
                if (buffer.isValid() && buffer.isDirty()) {
                    diskManager.WritePage(buffer.getPageId(), buffer.getData());
                }
            }
            // Reset all buffers
            for (Buffer buffer : bufferPool) {
                buffer.reset();
            }
            // Clear
            pageToBufferMap.clear();
            currentTime = 0;
        }
    }

    private int findReplacementBuffer() {
        // First, try to find an empty buffer
        for (int i = 0; i < bufferPool.length; i++) {
            if (!bufferPool[i].isValid()) {
                return i;
            }
        }
        // No empty buffer, need to apply replacement policy
        // Only consider unpinned buffers
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < bufferPool.length; i++) {
            if (!bufferPool[i].isPinned()) {
                candidates.add(i);
            }
        }

        if (candidates.isEmpty()) {
            throw new RuntimeException("No unpinned buffers available for replacement");
        }

        // Apply replacement policy
        if (currentPolicy.equals("LRU")) {
            return findLRUBuffer(candidates);
        } else { // MRU
            return findMRUBuffer(candidates);
        }
    }


    private int findLRUBuffer(List<Integer> candidates) {
        int lruIndex = candidates.get(0);
        long oldestTime = bufferPool[lruIndex].getLastAccessTime();

        for (int i = 1; i < candidates.size(); i++) {
            int candidateIndex = candidates.get(i);
            long candidateTime = bufferPool[candidateIndex].getLastAccessTime();
            if (candidateTime < oldestTime) {
                oldestTime = candidateTime;
                lruIndex = candidateIndex;
            }
        }

        return lruIndex;
    }

    private int findMRUBuffer(List<Integer> candidates) {
        int mruIndex = candidates.get(0);
        long newestTime = bufferPool[mruIndex].getLastAccessTime();

        for (int i = 1; i < candidates.size(); i++) {
            int candidateIndex = candidates.get(i);
            long candidateTime = bufferPool[candidateIndex].getLastAccessTime();
            if (candidateTime > newestTime) {
                newestTime = candidateTime;
                mruIndex = candidateIndex;
            }
        }

        return mruIndex;
    }

    public String getCurrentPolicy() { return currentPolicy; }
    public int getBufferPoolSize() { return bufferPool.length; }
    public int getLoadedPageCount() { return pageToBufferMap.size(); }


    public String getBufferPoolStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("Buffer Pool Status (Policy: ").append(currentPolicy).append(")\n");
        for (int i = 0; i < bufferPool.length; i++) {
            Buffer buffer = bufferPool[i];
            sb.append("Buffer ").append(i).append(": ");
            if (buffer.isValid()) {
                sb.append("PageId=").append(buffer.getPageId())
                        .append(", PinCount=").append(buffer.getPin_count())
                        .append(", Dirty=").append(buffer.isDirty())
                        .append(", LastAccess=").append(buffer.getLastAccessTime());
            } else {
                sb.append("EMPTY");
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
