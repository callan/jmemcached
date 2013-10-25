package com.thimbleware.jmemcached.test;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;

import org.junit.Test;
import org.omg.PortableInterceptor.SUCCESSFUL;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.util.Bytes;

public class StartupShutdown {

    protected static final int MAX_BYTES = (int) Bytes.valueOf("512m").bytes();
    public static final int CEILING_SIZE = (int) Bytes.valueOf("768m").bytes();
    public static final int MAX_SIZE = 10000;
    
    private MemCacheDaemon<LocalCacheElement> daemon;
        
    private MemCacheDaemon<LocalCacheElement> getDaemon (int port) {

        daemon = new MemCacheDaemon<LocalCacheElement>();
        
        CacheStorage<Key, LocalCacheElement> map = ConcurrentLinkedHashMap.create(
        						ConcurrentLinkedHashMap.EvictionPolicy.FIFO, MAX_SIZE, MAX_BYTES);
        
        daemon.setCache(new CacheImpl(map));
        daemon.setBinary(false);
        
        daemon.setAddr(new InetSocketAddress(port));
        daemon.setVerbose(false);
        
        return daemon;
    }
    
	@Test
	public void upAndDown() {
        daemon = getDaemon(800);
        
        try {
        	daemon.start();
        } catch (Exception e) {
        	fail("Daemon failed to start: " + e.getMessage());
        	return;
        }
        
        assertTrue( daemon.isRunning() );
        
        daemon.stop();
	}
}
