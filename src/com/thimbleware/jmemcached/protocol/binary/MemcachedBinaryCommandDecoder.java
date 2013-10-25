package com.thimbleware.jmemcached.protocol.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.protocol.CommandMessage;
import com.thimbleware.jmemcached.protocol.Op;
import com.thimbleware.jmemcached.protocol.exceptions.MalformedCommandException;
import com.thimbleware.jmemcached.util.BufferUtils;

/**
 * 
 */
public class MemcachedBinaryCommandDecoder extends MessageToMessageDecoder<ByteBuf> {
	private final Logger log = LogManager.getLogger(MemcachedBinaryCommandDecoder.class);
	
    public static final Charset USASCII = Charset.forName("US-ASCII");

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf channelBuffer, List<Object> out) throws Exception {
		log.trace("decode: " + BufferUtils.ByteBufToHex(channelBuffer));
		// Need at least 24 bytes to get header
		if (channelBuffer.readableBytes() < 24) {
			log.trace("readableBytes < 24");
			return;
		}
		
		// get the header
		channelBuffer.markReaderIndex();
		
		ByteBuf headerBuffer = Unpooled.buffer(24);
		channelBuffer.readBytes(headerBuffer);
		log.trace("header: " + BufferUtils.ByteBufToHex(headerBuffer));
		
		short magic = headerBuffer.readUnsignedByte();
		log.trace("magic byte: " + magic + " vs " + 0x80);
		
		// Magic Byte should be 0x80
		if (magic != 0x80) {
			headerBuffer.resetReaderIndex();
			throw new MalformedCommandException("Binary request payload is invalid, magic byte incorrect");
		}
		
        short opcode      = headerBuffer.readUnsignedByte();
        short keyLength   = headerBuffer.readShort();
        short extraLength = headerBuffer.readUnsignedByte();
        short dataType    = headerBuffer.readUnsignedByte(); // unused
        short reserved    = headerBuffer.readShort();        // unused
        
        int totalBodyLength = headerBuffer.readInt();
        
        int opaque = headerBuffer.readInt();
        long cas   = headerBuffer.readLong();

        // we want the whole of totalBodyLength; otherwise, keep waiting.
        if (channelBuffer.readableBytes() < totalBodyLength) {
            channelBuffer.resetReaderIndex();
            return;
        }
        
        // This assumes correct order in the enum. If that ever changes, we will have to scan for 'code' field.
        BinaryOp bcmd    = BinaryOp.values()[opcode];
        Op       cmdType = bcmd.correspondingOp;
        
        log.trace("command: " + cmdType);
        
        CommandMessage cmdMessage = CommandMessage.command(cmdType);
        
        cmdMessage.noreply = bcmd.noreply;
        cmdMessage.cas_key = cas;
        cmdMessage.opaque  = opaque;
        cmdMessage.addKeyToResponse = bcmd.addKeyToResponse;

        // get extras. could be empty.
        ByteBuf extrasBuffer = Unpooled.buffer(extraLength);
        channelBuffer.readBytes(extrasBuffer);

        log.trace("extras: " + BufferUtils.ByteBufToHex(extrasBuffer));
        
        // get the key if any
        if (keyLength != 0) {
            ByteBuf keyBuffer = Unpooled.buffer(keyLength);
            channelBuffer.readBytes(keyBuffer);

            log.trace("key: " + BufferUtils.ByteBufToStr(keyBuffer));
            
            ArrayList<Key> keys = new ArrayList<Key>();
            keys.add(new Key(keyBuffer.copy()));

            cmdMessage.keys = keys;

            if (cmdType == Op.ADD     ||
                cmdType == Op.SET     ||
                cmdType == Op.REPLACE ||
                cmdType == Op.APPEND  ||
                cmdType == Op.PREPEND) {
            	
                // TODO these are backwards from the spec, but seem to be what spymemcached demands -- which has the mistake?!
                short expire = (short) (extrasBuffer.capacity() != 0 ? extrasBuffer.readUnsignedShort() : 0);
                short flags  = (short) (extrasBuffer.capacity() != 0 ? extrasBuffer.readUnsignedShort() : 0);

                // the remainder of the message -- that is, totalLength - (keyLength + extraLength) should be the payload
                int size = totalBodyLength - (keyLength + extraLength);

                Key nKey = new Key(keyBuffer.slice());
                
                int expireOut = expire;

                if (expire != 0 && expire < CacheElement.THIRTY_DAYS) {
                	expireOut += LocalCacheElement.Now();
                }
                
                cmdMessage.element = new LocalCacheElement(nKey, flags, expireOut, 0L);
                
                ByteBuf data = Unpooled.buffer(size);
                channelBuffer.readBytes(data);
                cmdMessage.element.setData(data);
                
            } else if (cmdType == Op.INCR || cmdType == Op.DECR) {
                long initialValue = extrasBuffer.readUnsignedInt();
                long amount       = extrasBuffer.readUnsignedInt();
                long expiration   = extrasBuffer.readUnsignedInt();

                cmdMessage.incrAmount = (int) amount;
                cmdMessage.incrExpiry = (int) expiration;
            }
        }

        out.add(cmdMessage);
    }
}
