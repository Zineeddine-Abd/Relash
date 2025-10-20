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

            // Update mapping
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
                // Update access time for replacement policy
                if (currentPolicy.equals("LRU")) {
                    // For LRU, we don't update time on FreePage
                } else if (currentPolicy.equals("MRU")) {
                    // For MRU, we could update time, but typically MRU
                    // considers access time from GetPage
                }
            }
        }
    }

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

            // Clear page mapping
            pageToBufferMap.clear();

            // Reset time counter
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

    public String getCurrentPolicy() {
        return currentPolicy;
    }

    public int getBufferPoolSize() {
        return bufferPool.length;
    }

    public int getLoadedPageCount() {
        return pageToBufferMap.size();
    }
}