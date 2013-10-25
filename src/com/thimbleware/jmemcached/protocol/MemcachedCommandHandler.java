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
package com.thimbleware.jmemcached.protocol;


import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.DefaultChannelGroup;

import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.compat.log.LoggerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;

// TODO implement flush_all delay

/**
 * The actual command handler, which is responsible for processing the CommandMessage instances
 * that are inbound from the protocol decoders.
 * <p/>
 * One instance is shared among the entire pipeline, since this handler is stateless, apart from some globals
 * for the entire daemon.
 * <p/>
 * The command handler produces ResponseMessages which are destined for the response encoder.
 */

public final class MemcachedCommandHandler<CACHE_ELEMENT extends CacheElement> extends SimpleChannelInboundHandler<CommandMessage<CACHE_ELEMENT>> {
    final Logger logger = LogManager.getLogger(MemcachedCommandHandler.class);

    public final AtomicInteger curr_conns  = new AtomicInteger();
    public final AtomicInteger total_conns = new AtomicInteger();

    /**
     * The following state variables are universal for the entire daemon. These are used for statistics gathering.
     * In order for these values to work properly, the handler _must_ be declared with a ChannelPipelineCoverage
     * of "all".
     */
    public final String version;

    public final int idle_limit;
    public final boolean verbose;

    /**
     * The actual physical data storage.
     */
    private final Cache<CACHE_ELEMENT> cache;

    /**
     * The channel group for the entire daemon, used for handling global cleanup on shutdown.
     */
    private final DefaultChannelGroup channelGroup;

    /**
     * Construct the server session handler
     *
     * @param cache            the cache to use
     * @param memcachedVersion the version string to return to clients
     * @param verbosity        verbosity level for debugging
     * @param idle             how long sessions can be idle for
     * @param channelGroup
     */
    public MemcachedCommandHandler(Cache cache, String memcachedVersion, boolean verbosity, int idle, DefaultChannelGroup channelGroup) {
        this.cache = cache;

        version = memcachedVersion;
        verbose = verbosity;
        idle_limit = idle;
        this.channelGroup = channelGroup;
    }

    /**
     * On open we manage some statistics, and add this connection to the channel group.
     *
     * @param ctx
     * @param channelStateEvent
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        total_conns.incrementAndGet();
        curr_conns.incrementAndGet();
        channelGroup.add(ctx.channel());
    }

    /**
     * On close we manage some statistics, and remove this connection from the channel group.
     *
     * @param ctx
     * @param channelStateEvent
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        curr_conns.decrementAndGet();
        channelGroup.remove(ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
//    	logger.info("handler channelRead: " + obj);
    	channelRead0(ctx, (CommandMessage) obj);
    }

    /**
     * The actual meat of the matter.  Turn CommandMessages into executions against the physical cache, and then
     * pass on the downstream messages.
     *
     * @param ctx
     * @param messageEvent
     * @throws Exception
     */

    @Override
    public void channelRead0(ChannelHandlerContext ctx, CommandMessage command) throws Exception {
//    	logger.info("handler channelRead0: " + command.op);

    	Op cmd = command.op;
        
    	int cmdKeysSize = (command.keys == null ? 0 : command.keys.size());

        // first process any messages in the delete queue
        cache.asyncEventPing();

        // now do the real work
        if (this.verbose) {
            StringBuilder log = new StringBuilder();
            
            log.append(cmd);
            if (command.element != null) {
                log.append(" ").append(command.element.getKey().getName());
            }
            
            for (int i = 0; i < cmdKeysSize; i++) {
            	Key key = (Key) command.keys.get(i);
                log.append(" ").append(key.getName());
            }
            
            logger.info("LOGINFO: " + log.toString());
        }

        Channel channel = ctx.channel();
        
//      logger.info("CTX: " + ctx);
        
        if (cmd == null) {
        	handleNoOp(ctx, command);
        	return;
        }
        
        switch (cmd) {
            case GET:
            case GETS:
                handleGets(ctx, command, channel);
                break;
            case APPEND:
                handleAppend(ctx, command, channel);
                break;
            case PREPEND:
                handlePrepend(ctx, command, channel);
                break;
            case DELETE:
                handleDelete(ctx, command, channel);
                break;
            case DECR:
                handleDecr(ctx, command, channel);
                break;
            case INCR:
                handleIncr(ctx, command, channel);
                break;
            case REPLACE:
                handleReplace(ctx, command, channel);
                break;
            case ADD:
                handleAdd(ctx, command, channel);
                break;
            case SET:
                handleSet(ctx, command, channel);
                break;
            case CAS:
                handleCas(ctx, command, channel);
                break;
            case STATS:
                handleStats(ctx, command, cmdKeysSize, channel);
                break;
            case VERSION:
                handleVersion(ctx, command, channel);
                break;
            case QUIT:
                handleQuit(channel);
                break;
            case FLUSH_ALL:
                handleFlush(ctx, command, channel);
                break;
            case VERBOSITY:
                handleVerbosity(ctx, command, channel);
                break;
            default:
                 throw new UnknownCommandException("unknown command");
        }
    }

    protected void handleNoOp(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command) {
    	ctx.fireChannelRead( new ResponseMessage<CACHE_ELEMENT>(command) );
    }

    protected void handleFlush(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
    	ResponseMessage<CACHE_ELEMENT> msg = new ResponseMessage<CACHE_ELEMENT>(command).withFlushResponse(cache.flush_all(command.time));
    	
    	ctx.fireChannelRead(msg);
    }
    
    protected void handleVerbosity(ChannelHandlerContext ctx, CommandMessage command, Channel channel) {
    	ctx.fireChannelRead( new ResponseMessage<CACHE_ELEMENT>(command) );
 	}

    protected void handleQuit(Channel channel) {
        channel.disconnect();
    }

    protected void handleVersion(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        ResponseMessage<CACHE_ELEMENT> responseMessage = new ResponseMessage<CACHE_ELEMENT>(command);
        responseMessage.version = version;
        
        ctx.fireChannelRead(responseMessage);
    }

    protected void handleStats(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, int cmdKeysSize, Channel channel) {
        String option = "";
        
        if (cmdKeysSize > 0) {
            option = command.keys.get(0).getBytes().toString();
        }
        
		ResponseMessage<CACHE_ELEMENT> responseMessage = new ResponseMessage(command).withStatResponse(cache.stat(option));
        
        ctx.fireChannelRead(responseMessage);
    }

    protected void handleDelete(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.DeleteResponse dr = cache.delete(command.keys.get(0), command.time);
        ctx.fireChannelRead(new ResponseMessage(command).withDeleteResponse(dr));
    }

    protected void handleDecr(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Integer incrDecrResp = cache.get_add(command.keys.get(0), -1 * command.incrAmount);
        ctx.fireChannelRead(new ResponseMessage(command).withIncrDecrResponse(incrDecrResp));
    }

    protected void handleIncr(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Integer incrDecrResp = cache.get_add(command.keys.get(0), command.incrAmount); // TODO support default value and expiry!!
        ctx.fireChannelRead(new ResponseMessage(command).withIncrDecrResponse(incrDecrResp));
    }

    protected void handlePrepend(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.prepend(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleAppend(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.append(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleReplace(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.replace(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleAdd(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.add(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleCas(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.cas(command.cas_key, command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleSet(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.set(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleGets(ChannelHandlerContext ctx, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Key[] keys = new Key[command.keys.size()];
        
        keys = command.keys.toArray(keys);
        
        CACHE_ELEMENT[] results = get(keys);
        
        ResponseMessage<CACHE_ELEMENT> resp = new ResponseMessage<CACHE_ELEMENT>(command).withElements(results);
        
        ctx.fireChannelRead(resp);
    }

    /**
     * Get an element from the cache
     *
     * @param keys the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    private CACHE_ELEMENT[] get(Key... keys) {
        return cache.get(keys);
    }
}
