package com.thimbleware.jmemcached.protocol.text;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;

import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;
import com.thimbleware.jmemcached.protocol.SessionStatus;

/**
 */
public final class MemcachedPipelineFactory extends ChannelInitializer<SocketChannel> {

    final Logger log = LogManager.getLogger(MemcachedPipelineFactory.class);
	
    public static final Charset USASCII = Charset.forName("US-ASCII");

    private Cache cache;
    private String version;
    private boolean verbose;
    private int idleTime;
    private int frameSize;
    private DefaultChannelGroup chanGroup;

    public MemcachedPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, int frameSize, DefaultChannelGroup channelGroup) {
    	this.cache = cache;
    	this.version = version;
    	this.verbose = verbose;
    	this.idleTime = idleTime;
    	this.frameSize = frameSize;
    	this.chanGroup = channelGroup;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
    	log.trace("text initChannel");
    	SessionStatus   status   = new SessionStatus();
    	ChannelPipeline pipeline = ch.pipeline();
    	
    	pipeline.addLast("decoder", new MemcachedCommandDecoder(status));
    	pipeline.addLast("handler", new MemcachedCommandHandler(cache, version, verbose, idleTime, chanGroup));
    	pipeline.addLast("encoder", new MemcachedResponseEncoder());
    }
}
