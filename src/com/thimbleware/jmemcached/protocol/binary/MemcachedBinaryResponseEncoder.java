package com.thimbleware.jmemcached.protocol.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.protocol.Op;
import com.thimbleware.jmemcached.protocol.ResponseMessage;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;
import com.thimbleware.jmemcached.util.BufferUtils;

/**
 *
 */
// TODO refactor so this can be unit tested separate from netty? scalacheck?
@ChannelHandler.Sharable
public class MemcachedBinaryResponseEncoder<CACHE_ELEMENT extends CacheElement> 
				extends SimpleChannelInboundHandler<ResponseMessage<CACHE_ELEMENT>> {

    public static final Charset USASCII = Charset.forName("US-ASCII");
	
	private ConcurrentHashMap<Integer, ByteBuf> corkedBuffers = new ConcurrentHashMap<Integer, ByteBuf>();

    final Logger log = LogManager.getLogger(MemcachedBinaryResponseEncoder.class);

    public static enum ResponseCode {
        OK(0x0000),
        KEYNF(0x0001),
        KEYEXISTS(0x0002),
        TOOLARGE(0x0003),
        INVARG(0x0004),
        NOT_STORED(0x0005),
        UNKNOWN(0x0081),
        OOM(0x00082);

        public short code;

        ResponseCode(int code) {
            this.code = (short) code;
        }
    }

    public ResponseCode getStatusCode(ResponseMessage command) {
        Op cmd = command.cmd.op;
        
        if (cmd == Op.GET || cmd == Op.GETS) {
            return ResponseCode.OK;
            
        } else if (cmd == Op.SET     || cmd == Op.CAS     || cmd == Op.ADD     || 
        		   cmd == Op.REPLACE || cmd == Op.APPEND  || cmd == Op.PREPEND) {
        	
            switch (command.response) {
                case EXISTS:     return ResponseCode.KEYEXISTS;
                case NOT_FOUND:  return ResponseCode.KEYNF;
                case NOT_STORED: return ResponseCode.NOT_STORED;
                case STORED:     return ResponseCode.OK;
            }
            
        } else if (cmd == Op.INCR || 
        		   cmd == Op.DECR) {
        	
        	if (command.incrDecrResponse == null) {
        		return ResponseCode.KEYNF;
        	}
        	
        	return ResponseCode.OK;
        	
        } else if (cmd == Op.DELETE) {
        	
            switch (command.deleteResponse) {
                case DELETED:   return ResponseCode.OK;
                case NOT_FOUND: return ResponseCode.KEYNF;
            }
            
        } else if (cmd == Op.STATS   || 
        		   cmd == Op.VERSION ||
        		   cmd == Op.FLUSH_ALL) {
        	
            return ResponseCode.OK;
            
        }
        
        return ResponseCode.UNKNOWN;
    }

    public ByteBuf constructHeader(BinaryOp bcmd, ByteBuf extrasBuffer, ByteBuf keyBuffer, 
    					ByteBuf valueBuffer, short responseCode, int opaqueValue, long casUnique) {
    	
        short keyLength    = (short) getCap(keyBuffer);
        int   extrasLength = getCap(extrasBuffer);
        int   dataLength   = getCap(valueBuffer) + keyLength + extrasLength;
    	
    	// take the ResponseMessage and turn it into a binary payload.
    	ByteBuf header = Unpooled.buffer(24);

    	header.writeByte( (byte) 0x81 );         // magic
        header.writeByte( bcmd.code );           // opcode
        header.writeShort( keyLength );          // key length
        header.writeByte( (byte) extrasLength ); // extra length = flags + expiry
        header.writeByte( (byte) 0 );            // data type unused
        header.writeShort( responseCode );       // status code
        header.writeInt( dataLength );           // data length
        header.writeInt( opaqueValue );          // opaque
        header.writeLong( casUnique );           // cas value

        return header;
    }

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

    	if ( ! chan.isOpen() ) {
    		log.error("ERROR", cause);
    		return;
    	}
    	
    	if (cause instanceof UnknownCommandException) {
   			ByteBuf out = constructHeader(BinaryOp.Noop, null, null, null, (short) 0x0081, 0, 0);
   			chan.writeAndFlush(out);
    	} else {
    		log.error("ERROR", cause);
   			chan.close();
    	}
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object cmd) throws Exception {
    	channelRead0(ctx, (ResponseMessage<CACHE_ELEMENT>) cmd);
    }
    
    private int getCap (final ByteBuf buf) {
    	if (buf == null) {
    		return 0;
    	}
    	
    	return buf.capacity();
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, ResponseMessage<CACHE_ELEMENT> command) throws Exception {
        BinaryOp bcmd = BinaryOp.forCommandMessage(command.cmd);
        // write extras == flags & expiry
        ByteBuf extrasBuffer = null;
        // write key if there is one
        ByteBuf keyBuffer    = null;
        // write value if there is one
        ByteBuf valueBuffer  = null;
        // headerBuffer
        ByteBuf headerBuffer = null;

        if (bcmd.addKeyToResponse           && 
        	command.cmd.keys        != null && 
        	command.cmd.keys.size() != 0) {

        	keyBuffer = Unpooled.copiedBuffer( command.cmd.keys.get(0).getBytes() );
        }
        
        if (command.elements != null) {
            CacheElement element = command.elements[0];
            
        	extrasBuffer = Unpooled.buffer(4);
            extrasBuffer.writeShort((short) (element != null ? element.getExpire() : 0));
            extrasBuffer.writeShort((short) (element != null ? element.getFlags()  : 0));
            
            if (command.cmd.op == Op.GET || 
            	command.cmd.op == Op.GETS) {
            	
                if (element != null) {
                    valueBuffer = element.getData().duplicate();
                } else {
                    valueBuffer = Unpooled.buffer(0);
                }
            }
        }

        if (command.cmd.op == Op.INCR || 
            command.cmd.op == Op.DECR) {
        	
            valueBuffer = Unpooled.buffer(8);
            valueBuffer.writeLong(command.incrDecrResponse);
            
        }

        long casUnique = 0;
        
        if (command.elements        != null && 
        	command.elements.length != 0    && 
        	command.elements[0]     != null) {
        	
            casUnique = command.elements[0].getCasUnique();
            
        }

        // stats is special -- with it, we write N times, one for each stat, then an empty payload
        if (command.cmd.op == Op.STATS) {
            // first uncork any corked buffers
            if (corkedBuffers.containsKey(command.cmd.opaque)) {
            	uncork(command.cmd.opaque, ctx.channel());
            }

            for (Map.Entry<String, Set<String>> statsEntries : command.stats.entrySet()) {
                for (String stat : statsEntries.getValue()) {

                    keyBuffer    = Unpooled.copiedBuffer( statsEntries.getKey().getBytes(USASCII) );
                    valueBuffer  = Unpooled.copiedBuffer( stat.getBytes(USASCII) );
                    headerBuffer = constructHeader(bcmd, extrasBuffer, keyBuffer, valueBuffer, 
                    					getStatusCode(command).code, command.cmd.opaque, casUnique);

                    writePayload(ctx, extrasBuffer, keyBuffer, valueBuffer, headerBuffer);
                }
            }

            keyBuffer    = null;
            valueBuffer  = null;
            headerBuffer = constructHeader(bcmd, extrasBuffer, keyBuffer, valueBuffer, 
            					getStatusCode(command).code, command.cmd.opaque, casUnique);

            writePayload(ctx, extrasBuffer, keyBuffer, valueBuffer, headerBuffer);

        } else {
            headerBuffer = constructHeader(bcmd, extrasBuffer, keyBuffer, valueBuffer, 
            					getStatusCode(command).code, command.cmd.opaque, casUnique);

            // write everything
            // is the command 'quiet?' if so, then we append to our 'corked' buffer until a non-corked command comes along
            if (bcmd.noreply) {
            	int totalCapacity = headerBuffer.capacity() + getCap(extrasBuffer) + 
            						getCap(keyBuffer)       + getCap(valueBuffer);
            	
                ByteBuf corkedResponse = cork(command.cmd.opaque, totalCapacity);

                corkedResponse.writeBytes(headerBuffer);

                if (extrasBuffer != null) {
                    corkedResponse.writeBytes(extrasBuffer);
                }
                
                if (keyBuffer    != null) {
                    corkedResponse.writeBytes(keyBuffer);
                }
                
                if (valueBuffer  != null) {
                    corkedResponse.writeBytes(valueBuffer);
                }
            } else {
				// first write out any corked responses
				if (corkedBuffers.containsKey(command.cmd.opaque)) {
					uncork( command.cmd.opaque, ctx.channel() );
				}
				
				writePayload(ctx, extrasBuffer, keyBuffer, valueBuffer, headerBuffer);
            }
		}
	}

	private ByteBuf cork(int opaque, int totalCapacity) {
		if (corkedBuffers.containsKey(opaque)) {
			ByteBuf corkedResponse = corkedBuffers.get(opaque);
			ByteBuf oldBuffer      = corkedResponse;
			
			corkedResponse = Unpooled.buffer(totalCapacity + corkedResponse.capacity());
			corkedResponse.writeBytes(oldBuffer);
			
			oldBuffer.clear();
			
			corkedBuffers.remove(opaque);
			corkedBuffers.put(opaque, corkedResponse);
			
			return corkedResponse;
		} else {
			ByteBuf buffer = Unpooled.buffer(totalCapacity);
			
			corkedBuffers.put(opaque, buffer);
			
			return buffer;
		}
    }
	
	private void uncork(int opaque, Channel channel) {
		ByteBuf corkedBuffer = corkedBuffers.get(opaque);
		
		assert (corkedBuffer != null);
		
		channel.write(corkedBuffer.duplicate());
		corkedBuffers.remove(opaque);
	}

    private void writePayload(ChannelHandlerContext ctx, ByteBuf extrasBuffer, ByteBuf keyBuffer, ByteBuf valueBuffer, ByteBuf headerBuffer) {
    	log.trace("write payload");
    	Channel chan = ctx.channel();
    	
    	if ( ! chan.isOpen()) {
    		log.warn("channel isn't open");
    		return;
    	}
    	
    	log.trace("header: " + BufferUtils.ByteBufToHex(headerBuffer));
    	chan.write(headerBuffer);
    	
    	if (extrasBuffer != null) {
    		log.trace("extras: " + BufferUtils.ByteBufToHex(extrasBuffer));
    		chan.write(extrasBuffer);
    	}
    	
    	if (keyBuffer != null) {
    		log.trace("key: " + BufferUtils.ByteBufToHex(keyBuffer));
    		chan.write(keyBuffer);
    	}
    	
    	if (valueBuffer != null) {
    		log.trace("value: " + BufferUtils.ByteBufToHex(valueBuffer));
    		chan.write(valueBuffer);
    	}
    	
    	log.trace("flush");
    	chan.flush();
    }
}
