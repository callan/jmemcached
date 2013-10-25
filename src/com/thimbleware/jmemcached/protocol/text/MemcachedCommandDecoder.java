package com.thimbleware.jmemcached.protocol.text;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.protocol.CommandMessage;
import com.thimbleware.jmemcached.protocol.Op;
import com.thimbleware.jmemcached.protocol.SessionStatus;
import com.thimbleware.jmemcached.protocol.exceptions.IncorrectlyTerminatedPayloadException;
import com.thimbleware.jmemcached.protocol.exceptions.InvalidProtocolStateException;
import com.thimbleware.jmemcached.protocol.exceptions.MalformedCommandException;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;
import com.thimbleware.jmemcached.util.BufferUtils;

/**
 * The MemcachedCommandDecoder is responsible for taking lines from the MemcachedFrameDecoder and parsing them
 * into CommandMessage instances for handling by the MemcachedCommandHandler
 * <p/>
 * Protocol status is held in the SessionStatus instance which is shared between each of the decoders in the pipeline.
 */
public final class MemcachedCommandDecoder extends ByteToMessageDecoder {

	static final Logger log = LogManager.getLogger(MemcachedCommandDecoder.class);
	
    private static final int MIN_BYTES_LINE = 2;
    private SessionStatus status;

    private static final byte[] NOREPLY = "noreply".getBytes();
    
    public MemcachedCommandDecoder(SessionStatus status) {
        this.status = status;
    }
    
    /**
     * Index finder which locates a byte which is neither a {@code CR ('\r')}
     * nor a {@code LF ('\n')} nor a {@code SPACE (' ')}.
     */
    final static ByteBufProcessor CRLF_OR_WS = new ByteBufProcessor() {
        public final boolean process (final byte bt) {
        	if (bt == ' ' || bt == '\r' || bt == '\n') {
        		return false;
        	}
        	
        	return true;
        }
    };
    
    final static int bytesBeforeWorkAround (ByteBuf buffer) {
    	int  k = -1;
    	char c;
    	
    	for (int i = buffer.readerIndex(); i < buffer.capacity(); i++) {
    		c = (char) buffer.getByte(i);
    		
//    		System.out.println("'" + c + "' " + (c == ' ' || c == '\r' || c == '\n'));
    		
    		if (c == ' ' || c == '\r' || c == '\n') {
    			k = i;
    			break;
    		}
    	}
    	
    	return k;
    }
    
    static boolean eol(int pos, ByteBuf buffer) {
        return (buffer.readableBytes() >= MIN_BYTES_LINE &&
        		buffer.getByte(buffer.readerIndex() + pos    ) == '\r' &&
        		buffer.getByte(buffer.readerIndex() + pos + 1) == '\n');
    }

    public SplitList splitToList (ByteBuf buff) {
    	List<ByteBuf> pieces = new ArrayList<ByteBuf>(6);
    	
//    	log.info("buff = '" + BufferUtils.ByteBufToStr(buff) + "'");
   	
    	int     pos  = bytesBeforeWorkAround(buff);
    	int     len  = 0;
    	boolean eol  = false;
    	int     skip = 1;
    	

        ByteBuf slice = null;
        
        do {
//        	log.info("do/while pos=" + pos + ",ind=" + buff.readerIndex());
            if (pos == -1) {
                continue;
            }

            len  = (pos - buff.readerIndex());
            eol  = eol(len, buff);
            skip = (eol ? MIN_BYTES_LINE : 1);

//            log.info("eol=" + eol);
//            log.info("len=" + len);
            
            slice = buff.slice(buff.readerIndex(), len);
            slice.readerIndex(0);
            
//            log.info("slice[" + pieces.size() + "] = " + BufferUtils.ByteBufToStr(slice));
            
            pieces.add(slice);
            
            buff.skipBytes(len + skip);
            
            if (eol) {
            	break;
            }
        } while ( (pos = bytesBeforeWorkAround(buff)) != -1 );
        
        return new SplitList(pieces, eol);
    }
    
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
    	log.info("DECODE: " + status.state);
    	log.info("DECODE: " + BufferUtils.ByteBufToStr(buffer));

        if (status.state == SessionStatus.State.READY) {
        	ByteBuf in = buffer.slice();
        	
        	if (in.readableBytes() < MIN_BYTES_LINE) {
        		return;
        	}
        	
        	SplitList sList = splitToList(in);
        	
        	if (sList == null) {
        		return;
        	}
        	
        	List<ByteBuf> pieces = sList.getList();

            if (sList.hasEof()) {
                buffer.skipBytes(in.readerIndex());

//              log.info("Running processLine");
                Object outz = processLine(pieces, ctx);
//              log.info("outz = " + outz);
                
                if (outz != null) {
                	out.add(outz);
                }
                
//              in = in.copy();
//              log.info("STATUS: " + status.state);
                return;
            }

            if (status.state != SessionStatus.State.WAITING_FOR_DATA) {
            	status.ready();
            }
            
        } else if (status.state == SessionStatus.State.WAITING_FOR_DATA) {
        	log.info("WAITING_FOR_DATA!");
        	log.info("bytesNeeded: " + (status.bytesNeeded + 2));
        	log.info("readableBytes: " + buffer.readableBytes());
        	
            if (buffer.readableBytes() >= status.bytesNeeded + 2) {
                // verify delimiter matches at the right location
                ByteBuf dest = buffer.slice(buffer.readerIndex() + status.bytesNeeded, MIN_BYTES_LINE);

                log.info("DEST = " + BufferUtils.ByteBufToStr(dest));
                
                if ( ! BufferUtils.ByteBufToStr(dest).equals("#&")) {
                    // before we throw error... we're ready for the next command
                    status.ready();

//                  log.warn("BAD PAYLOAD");
                    
                    // error, no delimiter at end of payload
                    throw new IncorrectlyTerminatedPayloadException("payload not terminated correctly");
                } else {
                    status.processingMultiline();

//                  log.info("GOOD PAYLOAD");
                    
                    // There's enough bytes in the buffer and the delimiter is at the end. Read it.
                    
                    ByteBuf result = Unpooled.buffer(status.bytesNeeded);
                    buffer.getBytes(buffer.readerIndex(), result, status.bytesNeeded);
                    //ByteBuf result = buffer.copy(buffer.readerIndex(), status.bytesNeeded);

                    log.info("RESULT = " + BufferUtils.ByteBufToStr(result));
                    
                    buffer.skipBytes(status.bytesNeeded + MemcachedResponseEncoder.CRLF.length);
                    
                    CommandMessage commandMessage = continueSet(ctx.channel(), status, result, ctx);

                    if (status.state != SessionStatus.State.WAITING_FOR_DATA) {
                    	status.ready();
                    }

                    out.add(commandMessage);
                    return;
                }
            }
        } else {
            throw new InvalidProtocolStateException("invalid protocol state");
        }
    }

    /**
     * Process an individual complete protocol line and either passes the command for processing by the
     * session handler, or (in the case of SET-type commands) partially parses the command and sets the session into
     * a state to wait for additional data.
     *
     * @param parts   the (originally space separated) parts of the command
     * @param channel the netty channel to operate on
     * @param ctx     the netty channel handler context
     * @throws com.thimbleware.jmemcached.protocol.exceptions.MalformedCommandException
     * @throws com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException
     */
    private Object processLine(List<ByteBuf> parts, ChannelHandlerContext ctx) throws UnknownCommandException, MalformedCommandException {
        final int numParts = parts.size();
//      log.info("parts: " + parts.size());
        
//      for (ByteBuf part : parts) {
//      	log.info("part: " + BufferUtils.ByteBufToStr(part));
//      }

        // Turn the command into an enum for matching on
        Op op;

        try {
            op = Op.FindOp(parts.get(0));

            if (op == null) {
                throw new IllegalArgumentException("[1] unknown operation: " + parts.get(0).toString());
            }

        } catch (IllegalArgumentException e) {
            throw new UnknownCommandException("[2] unknown operation: " + parts.get(0).toString());
        }

        // Produce the initial command message, for filling in later
        CommandMessage cmd = CommandMessage.command(op);

        switch (op) {
            case DELETE:
                cmd.setKey(parts.get(1));

                if (numParts >= MIN_BYTES_LINE) {
                    if (byteBufEq(parts.get(numParts - 1), NOREPLY)) {
                        cmd.noreply = true;
                        if (numParts == 4) {
                            cmd.time = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                        }
                    } 
                    else if (numParts == 3) {
                        cmd.time = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                    }
                }

                return cmd;
            case DECR:
            case INCR:
                // Malformed
                if (numParts < MIN_BYTES_LINE || numParts > 3) {
                    throw new MalformedCommandException("invalid increment command");
                }

                cmd.setKey(parts.get(1));
                cmd.incrAmount = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));

                if (numParts == 3 && byteBufEq(parts.get(MIN_BYTES_LINE), NOREPLY)) {
                    cmd.noreply = true;
                }

                return cmd;
            case FLUSH_ALL:
            	if (numParts == 1) {
            		return cmd;
            	}
            	
            	
                if (byteBufEq(parts.get(numParts - 1), NOREPLY)) {
                    cmd.noreply = true;
                    if (numParts == 3) {
                        cmd.time = BufferUtils.atoi((parts.get(1)));
                    }
                } else if (numParts == MIN_BYTES_LINE) {
                    cmd.time = BufferUtils.atoi((parts.get(1)));
                }

                return cmd;
            case VERBOSITY: // verbosity <time> [noreply]\r\n
                // Malformed
                if (numParts < MIN_BYTES_LINE || numParts > 3) {
                    throw new MalformedCommandException("invalid verbosity command");
                }

                cmd.time = BufferUtils.atoi(parts.get(1)); // verbose level

                if (numParts > 1 && parts.get(MIN_BYTES_LINE).equals(NOREPLY)) {
                    cmd.noreply = true;
                }

                return cmd;
            case APPEND:
            case PREPEND:
            case REPLACE:
            case ADD:
            case SET:
            case CAS:
            	log.info("SET/CAS/ETC.");
                // if we don't have all the parts, it's malformed
                if (numParts < 5) {
                    throw new MalformedCommandException("invalid command length");
                }

                // Fill in all the elements of the command
                int size    = BufferUtils.atoi(parts.get(4));
                int expire  = BufferUtils.atoi(parts.get(3));
                int flags   = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                
                log.info("SIZE:   " + size + ", "
                	+    "EXPIRE: " + expire + ", "
                	+    "FLAGS:  " + flags + "!");
                
                cmd.element = new LocalCacheElement(new Key(parts.get(1).slice()), flags, expire != 0 && expire < CacheElement.THIRTY_DAYS ? LocalCacheElement.Now() + expire : expire, 0L);

                log.info("ELEM: " + cmd.element);
                
                // look for cas and "noreply" elements
                if (numParts > 5) {
                    int noreply = (op == Op.CAS ? 6 : 5);
                    
                    if (op == Op.CAS) {
                        cmd.cas_key = BufferUtils.atol(parts.get(5));
                    }

                    if (numParts == noreply + 1 && byteBufEq(parts.get(noreply), NOREPLY)) {
                        cmd.noreply = true;
                    }
                }

                // Now indicate that we need more for this command by changing the session status's state.
                // This instructs the frame decoder to start collecting data for us.
                status.needMore(size, cmd);
                break;

            //
            case GET:
            case GETS:
            case STATS:
            case VERSION:
            case QUIT:
                // Get all the keys
                cmd.setKeys(parts.subList(1, numParts));

                // Pass it on.
                return cmd;
            default:
                throw new UnknownCommandException("unknown command: " + op);
        }

        return null;
    }

    private boolean byteBufEq (ByteBuf left, byte[] right) {
    	ByteBuf rightBuf = Unpooled.wrappedBuffer(right);
    	return left.equals(rightBuf);
    }
    
    /**
     * Handles the continuation of a SET/ADD/REPLACE command with the data it was waiting for.
     *
     * @param channel               netty channel
     * @param state                 the current session status (unused)
     * @param remainder             the bytes picked up
     * @param channelHandlerContext netty channel handler context
     */
    private CommandMessage continueSet(Channel channel, SessionStatus state, ByteBuf remainder, ChannelHandlerContext channelHandlerContext) {
//    	log.info("CSET  = " + remainder.toString(Charset.defaultCharset()));
    	
        state.cmd.element.setData(remainder);
        return state.cmd;
    }
}
