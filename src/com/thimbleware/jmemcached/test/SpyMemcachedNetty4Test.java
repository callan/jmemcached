/**
 * 
 */
package com.thimbleware.jmemcached.test;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.Future;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.util.Bytes;

/**
 * @author Chris
 * 
 */
public class SpyMemcachedNetty4Test {

	private MemcachedClient client;
	private InetSocketAddress address;
	private MemCacheDaemon<LocalCacheElement> daemon;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		int port = AvailablePortFinder.getNextAvailable();
		
		this.daemon  = new MemCacheDaemon<LocalCacheElement>();
		this.address = new InetSocketAddress("localhost", port);

		CacheStorage<Key, LocalCacheElement> map = ConcurrentLinkedHashMap
				.create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO,
						(int) Bytes.valueOf("128m").bytes(),
						Bytes.valueOf("512m").bytes());

		daemon.setCache(new CacheImpl(map));
		daemon.setAddr(this.address);
		daemon.start();
		
		this.client = new MemcachedClient(Arrays.asList(address));
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		daemon.stop();
		if (client != null) {
			client.shutdown();
		}
	}

	@Test
	public void test() throws Exception {
		Future<Boolean> future = client.set("foo", 32000, "123");
		
		assertTrue(future.get());
		
		CASValue<Object> casValue = client.gets("foo");
		
		assertEquals( "123", casValue.getValue() );
	}

}
