package com.thimbleware.jmemcached.protocol.text;

import static com.thimbleware.jmemcached.protocol.text.MemcachedPipelineFactory.USASCII;
import static java.lang.String.valueOf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.protocol.Op;
import com.thimbleware.jmemcached.protocol.ResponseMessage;
import com.thimbleware.jmemcached.protocol.exceptions.ClientException;
import com.thimbleware.jmemcached.util.BufferUtils;

/**
 * Response encoder for the memcached text protocol. Produces strings destined for the StringEncoder
 */
public final class MemcachedResponseEncoder<CACHE_ELEMENT extends CacheElement> 
					extends SimpleChannelInboundHandler<ResponseMessage<CACHE_ELEMENT>> {

    final Logger logger = LogManager.getLogger(MemcachedResponseEncoder.class);

    public  static final byte[] CRLF = "\r\n".getBytes();
    private static final byte[] VALUE = "VALUE ".getBytes();
    private static final byte[] EXISTS = "EXISTS\r\n".getBytes();
    private static final byte[] NOT_FOUND = "NOT_FOUND\r\n".getBytes();
    private static final byte[] NOT_STORED = "NOT_STORED\r\n".getBytes();
    private static final byte[] STORED = "STORED\r\n".getBytes();
    private static final byte[] DELETED = "DELETED\r\n".getBytes();
    private static final byte[] END = "END\r\n".getBytes();
    private static final byte[] OK = "OK\r\n".getBytes();
    private static final byte[] ERROR = "ERROR\r\n".getBytes();
    private static final byte[] CLIENT_ERROR = "CLIENT_ERROR\r\n".getBytes();

    /**
     * Handle exceptions in protocol processing. Exceptions are either client or internal errors.  Report accordingly.
     *
     * @param ctx
     * @param e
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    	Channel chan = ctx.channel();
    	
    	if (cause instanceof ClientException) {
    		if (chan.isOpen()) {
    			chan.write(CLIENT_ERROR);
    		}
    	} else if (cause instanceof IOException) {
    		logger.warn("IOException: " + cause.getMessage());
    	} else {
    		logger.error("cause unknown error", cause);
    		if (chan.isOpen()) {
    			chan.write(ERROR);
    		}
    	}
    }

    @Override
    public void channelRegistered (ChannelHandlerContext ctx) throws Exception {
    	logger.info("textEncoder channelRegistered");
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object cmd) throws Exception {
    	logger.info("textEncoder channelRead: " + cmd);
    	channelRead0(ctx, (ResponseMessage<CACHE_ELEMENT>) cmd);
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, ResponseMessage<CACHE_ELEMENT> command) throws Exception {
    	logger.info("textEncoder channelRead0: " + command);

    	Op cmd = command.cmd.op;

    	logger.info("noreply? " + command.cmd.noreply);
    	
    	if (command.cmd.noreply) {
    		return;
    	}
    	
        switch (cmd) {
            case GET:
            case GETS:
                CacheElement[] results = command.elements;
                logger.info("GET/GETS: " + command.elements.length);
                ByteBuf out = Unpooled.compositeBuffer( (results.length * 3) + 1 );
                
                int i = 0;
                for (CacheElement result : results) {
                	logger.info("WRITE: " + (i++));
                	if (result == null) {
                		continue;
                	}
                	
                	out.writeBytes(VALUE);
                	out.writeBytes(result.getKey().getBytes());
                	out.writeByte(' ');
                	out.writeBytes( BufferUtils.itoa(result.getFlags()) );
                	out.writeByte(' ');
                	out.writeBytes( BufferUtils.itoa(result.size()) );
                	out.writeByte(' ');
                	
                	if (cmd == Op.GETS){
                    	
                		out.writeBytes( BufferUtils.ltoa(result.getCasUnique()) );
                		out.writeByte(' ');
                	}
                	
                	out.writeBytes(CRLF);
                	out.writeBytes(result.getData());
                	out.writeBytes(CRLF);
                }
                
                out.writeBytes(END);
                logger.info("SEND OUT!");
                logger.info( BufferUtils.ByteBufToStr(out) );
                
                writeOut(ctx, out);
                
                break;
            case APPEND:
            case PREPEND:
            case ADD:
            case SET:
            case REPLACE:
            case CAS:
               	writeOut(ctx, storeResponse(command.response));
                break;
            case DELETE:
            	writeOut(ctx, deleteResponseString(command.deleteResponse));
                break;
            case DECR:
            case INCR:
            	writeOut(ctx, incrDecrResponseString(command.incrDecrResponse));
                break;
            case STATS:
            	ByteBuf statsOut = Unpooled.compositeBuffer( (command.stats.entrySet().size() * 6) + 1 );
                for (Map.Entry<String, Set<String>> stat : command.stats.entrySet()) {
                    for (String statVal : stat.getValue()) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("STAT ");
                        builder.append(stat.getKey());
                        builder.append(" ");
                        builder.append(String.valueOf(statVal));
                        builder.append("\r\n");
                        
                        statsOut.writeBytes(builder.toString().getBytes());
                    }
                }
                
                statsOut.writeBytes(END);
                writeOut(ctx, statsOut);
                break;
            case VERSION:
            	writeOut(ctx, "VERSION " + command.version + "\r\n");
                break;
            case QUIT:
                ctx.disconnect();

                break;
            case FLUSH_ALL:
               	byte[] flushOut = command.flushSuccess ? OK : ERROR;
               	writeOut(ctx, flushOut);
                break;
            case VERBOSITY:
                break;
            default:
                writeOut(ctx, ERROR);
                logger.error("error; unrecognized command: " + cmd);
        }
    }

    private void writeOut (ChannelHandlerContext ctx, String out) {
    	writeOut(ctx, Unpooled.wrappedBuffer(out.getBytes()));
    }
    
    private void writeOut (ChannelHandlerContext ctx, ByteBuf out) {
    	logger.info("WRITE OUT: " + BufferUtils.ByteBufToStr(out));
    	ctx.write(out);
    	ctx.flush();
    }
    
    private void writeOut (ChannelHandlerContext ctx, byte[] out) {
    	writeOut(ctx, Unpooled.wrappedBuffer(out));
    }
    
    private byte[] deleteResponseString(Cache.DeleteResponse deleteResponse) {
        if (deleteResponse == Cache.DeleteResponse.DELETED) {
        	return DELETED;
        } else {
        	return NOT_FOUND;
        }
    }


    private byte[] incrDecrResponseString(Integer ret) {
        if (ret == null) {
            return NOT_FOUND;
        } else {
        	return (valueOf(ret) + "\r\n").getBytes();
        }
    }

    /**
     * Find the string response message which is equivalent to a response to a set/add/replace message
     * in the cache
     *
     * @param storeResponse the response code
     * @return the string to output on the network
     */
    private byte[] storeResponse(Cache.StoreResponse storeResponse) {
    	byte[] out = null;
    	
        switch (storeResponse) {
            case EXISTS:
                out = EXISTS;
                break;
            case NOT_FOUND:
                out = NOT_FOUND;
                break;
            case NOT_STORED:
                out = NOT_STORED;
                break;
            case STORED:
                out = STORED;
                break;
            default:
                throw new RuntimeException("unknown store response from cache: " + storeResponse);
        }
        
        return out;
    }
}
