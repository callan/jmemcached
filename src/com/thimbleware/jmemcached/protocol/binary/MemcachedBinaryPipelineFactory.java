package com.thimbleware.jmemcached.protocol.binary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;
import com.thimbleware.jmemcached.protocol.text.MemcachedPipelineFactory;

public class MemcachedBinaryPipelineFactory extends ChannelInitializer<SocketChannel> {
    final Logger log = LogManager.getLogger(MemcachedPipelineFactory.class);

    private Cache cache;
    private String version;
    private boolean verbose;
    private int idleTime;
    private DefaultChannelGroup chanGroup;
    
    public MemcachedBinaryPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, DefaultChannelGroup channelGroup) {
    	this.cache     = cache;
    	this.version   = version;
    	this.verbose   = verbose;
    	this.idleTime  = idleTime;
    	this.chanGroup = channelGroup;
    	
        log.trace("binary constructor");
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
    	log.trace("binary initChannel");
    	ChannelPipeline pipeline = ch.pipeline();
    	
    	pipeline.addLast("decoder", new MemcachedBinaryCommandDecoder());
    	pipeline.addLast("handler", new MemcachedCommandHandler(cache, version, verbose, idleTime, chanGroup));
    	pipeline.addLast("encoder", new MemcachedBinaryResponseEncoder());

    }
    
}
