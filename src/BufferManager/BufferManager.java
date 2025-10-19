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
}