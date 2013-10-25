package com.thimbleware.jmemcached;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Represents a given key for lookup in the cache.
 *
 * Wraps a byte array with a precomputed hashCode.
 */
public final class Key {
    private ByteBuf bytes;
    private byte[] bytesOut;
    
    private int hashCode;

    public Key(ByteBuf bytes) {
        this.bytes    = bytes;
        this.hashCode = this.bytes.hashCode();
        this.convertBytes();
    }

    private void convertBytes () {
    	bytesOut = new byte[bytes.readableBytes()];
    	bytes.readBytes(bytesOut);
    }
    
    @Override
    public boolean equals(Object obj) {
    	if (this == obj) {
    		return true;
    	}
    	
    	if (obj == null || getClass() != obj.getClass()) {
    		return false;
    	}

        final Key ext = (Key) obj;
        final byte[] extBytes = ext.getBytes();

        if (extBytes.length != bytesOut.length) {
        	return false;
        }
        
        for (int i = 0; i < extBytes.length; i++) {
        	if (extBytes[i] != this.bytesOut[i]) {
        		return false;
        	}
        }
        
        return true;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    public byte[] getBytes () {
    	return bytesOut;
    }
    
    public String getName () {
    	return bytes.toString(Charset.defaultCharset());
    }
}
