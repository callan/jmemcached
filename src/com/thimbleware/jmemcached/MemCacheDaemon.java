/**
 *  Copyright 2008 ThimbleWare Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.thimbleware.jmemcached;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.thimbleware.jmemcached.protocol.binary.MemcachedBinaryPipelineFactory;
import com.thimbleware.jmemcached.protocol.text.MemcachedPipelineFactory;

/**
 * The actual daemon - responsible for the binding and configuration of the network configuration.
 */
public class MemCacheDaemon<CACHE_ELEMENT extends CacheElement> {

    final Logger log = LogManager.getLogger(MemCacheDaemon.class);

    public static String memcachedVersion = "1.0";

    private int frameSize = 32768 * 1024;

    private boolean binary = false;
    private boolean verbose;
    private int idleTime;
    private InetSocketAddress addr;
    private Cache<CACHE_ELEMENT> cache;

    private boolean running = false;
    private ServerBootstrap bootstrap;
    private DefaultChannelGroup allChannels;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workGroup;

    public MemCacheDaemon() {
    }

    public MemCacheDaemon(Cache<CACHE_ELEMENT> cache) {
        this.cache = cache;
    }

    /**
     * Bind the network connection and start the network processing threads.
     */
    public void start() throws Exception {
    	log.info("starting jmemcached");

    	bossGroup = new NioEventLoopGroup();
    	workGroup = new NioEventLoopGroup();
    	
    	log.info("loop groups setup");
    	
		allChannels = new DefaultChannelGroup("jmemcached", GlobalEventExecutor.INSTANCE);
		
		ChannelInitializer<?> init;
		
		if (binary) {
			init = createMemcachedBinaryPipelineFactory(cache, memcachedVersion, verbose, idleTime, allChannels);
		} else {
			init = createMemcachedPipelineFactory(cache, memcachedVersion, verbose, idleTime, 65536, allChannels);
		}
		
		log.info("factory setup: " + ( binary ? "binary" : "plaintext"));
		
		bootstrap = new ServerBootstrap();

		log.info("group/channel/childHandler");    		
		bootstrap.group(bossGroup, workGroup)
			     .channel(NioServerSocketChannel.class)
			     .childHandler(init);

		log.info("childOptions");
		bootstrap.childOption(ChannelOption.SO_RCVBUF, 65536)
			     .childOption(ChannelOption.SO_SNDBUF, 65536);
		
		log.info("bind/sync/channel");
		bootstrap.localAddress(addr);
		ChannelFuture future = bootstrap.bind(addr);
		
		Channel chan = future.syncUninterruptibly().channel();
		
		log.info("chan? " + chan.isActive());
		
		log.info("add channels");
		allChannels.add(chan);
		
		// This blocks the rest of the method from running until the server stops.
		// TODO find away around this (as Netty 3.x did this a very different way)
		// chan.closeFuture().sync();
		log.info("done");

        log.info("listening on " + String.valueOf(addr.getHostName()) + ":" + addr.getPort());
        running = true;
    }

    protected ChannelInitializer<?> createMemcachedBinaryPipelineFactory(
            Cache<?> cache, String memcachedVersion, boolean verbose, int idleTime, DefaultChannelGroup allChannels) {
        return new MemcachedBinaryPipelineFactory(cache, memcachedVersion, verbose, idleTime, allChannels);
    }

    protected ChannelInitializer<?> createMemcachedPipelineFactory(
            Cache<?> cache, String memcachedVersion, boolean verbose, int idleTime, int receiveBufferSize, DefaultChannelGroup allChannels) {
        return new MemcachedPipelineFactory(cache, memcachedVersion, verbose, idleTime, receiveBufferSize, allChannels);
    }

    public void stop() {
        log.info("terminating daemon; closing all channels");

        bossGroup.shutdownGracefully();
        workGroup.shutdownGracefully();        
        ChannelGroupFuture future = allChannels.close();
        try {
			future.await(50);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
        
        if ( ! future.isSuccess() ) {
            throw new RuntimeException("failure to complete closing all network channels");
        }
        
        log.info("channels closed, freeing cache storage");
        try {
            cache.close();
        } catch (IOException e) {
            throw new RuntimeException("exception while closing storage", e);
        }

        running = false;
        log.info("successfully shut down");
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void setIdleTime(int idleTime) {
        this.idleTime = idleTime;
    }

    public void setAddr(InetSocketAddress addr) {
        this.addr = addr;
    }

    public Cache<CACHE_ELEMENT> getCache() {
        return cache;
    }

    public void setCache(Cache<CACHE_ELEMENT> cache) {
        this.cache = cache;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isBinary() {
        return binary;
    }

    public void setBinary(boolean binary) {
        this.binary = binary;
    }
}
