package com.thimbleware.jmemcached.test;

import com.thimbleware.jmemcached.*;
import com.thimbleware.jmemcached.storage.bytebuffer.BlockStorageCacheStorage;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferBlockStore;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;
import com.thimbleware.jmemcached.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

/**
 */
public abstract class AbstractCacheTest {
    protected static final int MAX_BYTES = (int) Bytes.valueOf("4m").bytes();
    public static final int CEILING_SIZE = (int) Bytes.valueOf("4m").bytes();
    public static final int MAX_SIZE = 1000;
    protected MemCacheDaemon<LocalCacheElement> daemon;
    private int port;
    protected Cache<LocalCacheElement> cache;
    protected final CacheType cacheType;
    protected final int blockSize;
    private final ProtocolMode protocolMode;

    public AbstractCacheTest(CacheType cacheType, int blockSize, ProtocolMode protocolMode) {
        this.blockSize = blockSize;
        this.cacheType = cacheType;
        this.protocolMode = protocolMode;
    }


    public static enum CacheType {
        LOCAL_HASH, BLOCK, MAPPED
    }

    public static enum ProtocolMode {
        TEXT, BINARY
    }

    @Parameterized.Parameters
    public static Collection blockSizeValues() {
        return Arrays.asList(new Object[][] {
                {CacheType.LOCAL_HASH, 1, ProtocolMode.TEXT },
                {CacheType.LOCAL_HASH, 1, ProtocolMode.BINARY },
                {CacheType.BLOCK, 4, ProtocolMode.TEXT},
                {CacheType.BLOCK, 4, ProtocolMode.BINARY},
                {CacheType.MAPPED, 4, ProtocolMode.TEXT},
                {CacheType.MAPPED, 4, ProtocolMode.BINARY }
        });
    }

    @Before
    public void setup() throws IOException {
        // create daemon and start it
        daemon = new MemCacheDaemon<LocalCacheElement>();
        CacheStorage<Key, LocalCacheElement> cacheStorage = getCacheStorage();

        daemon.setCache(new CacheImpl(cacheStorage));
        daemon.setBinary(protocolMode == ProtocolMode.BINARY);
        
        port = AvailablePortFinder.getNextAvailable();
        daemon.setAddr(new InetSocketAddress(port));
        daemon.setVerbose(true);
        
        try {
        	daemon.start();
        } catch (Exception e) {
        	e.printStackTrace();
        	System.exit(1);
        }

        cache = daemon.getCache();
    }


    @After
    public void teardown() {
        if (daemon.isRunning())
            daemon.stop();
    }

    private CacheStorage<Key, LocalCacheElement> getCacheStorage() throws IOException {
        CacheStorage<Key, LocalCacheElement> cacheStorage = null;
        switch (cacheType) {
            case LOCAL_HASH:
                cacheStorage = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, MAX_SIZE, MAX_BYTES);
                break;
            case BLOCK:
                cacheStorage = new BlockStorageCacheStorage(16, CEILING_SIZE, blockSize, MAX_BYTES, MAX_SIZE, new ByteBufferBlockStore.ByteBufferBlockStoreFactory());
                break;
            case MAPPED:
                cacheStorage = new BlockStorageCacheStorage(16, CEILING_SIZE, blockSize, MAX_BYTES, MAX_SIZE, MemoryMappedBlockStore.getFactory());

                break;
        }
        return cacheStorage;
    }

    public MemCacheDaemon getDaemon() {
        return daemon;
    }

    public Cache getCache() {
        return cache;
    }

    public CacheType getCacheType() {
        return cacheType;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public ProtocolMode getProtocolMode() {
        return protocolMode;
    }

    public int getPort() {
        return port;
    }
}