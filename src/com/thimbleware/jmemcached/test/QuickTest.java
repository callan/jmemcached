package com.thimbleware.jmemcached.test;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.bytebuffer.BlockStorageCacheStorage;
import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferBlockStore;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;
import com.thimbleware.jmemcached.test.AbstractCacheTest.ProtocolMode;
import com.thimbleware.jmemcached.util.Bytes;

public class QuickTest {
    protected static final int MAX_BYTES = (int) Bytes.valueOf("4m").bytes();
    public static final int CEILING_SIZE = (int) Bytes.valueOf("4m").bytes();
    public static final int MAX_SIZE = 1000;
	
	public static void main(String[] args) {
        // create daemon and start it
		Logger log = LogManager.getLogger("QuickTest");
		
        MemCacheDaemon daemon = new MemCacheDaemon<LocalCacheElement>();
        CacheStorage<Key, LocalCacheElement> map = ConcurrentLinkedHashMap.create(
        						ConcurrentLinkedHashMap.EvictionPolicy.FIFO, MAX_SIZE, MAX_BYTES);
        
        daemon.setCache(new CacheImpl(map));
        daemon.setBinary(false);
        
        daemon.setAddr(new InetSocketAddress(11211));
        daemon.setVerbose(false);
        
        try {
        	daemon.start();
        } catch (Exception e) {
        	e.printStackTrace();
        	return;
        }

        Cache cache = daemon.getCache();
	}

}
