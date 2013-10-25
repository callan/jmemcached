package com.thimbleware.jmemcached.test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ByteBufTest {
	public static void main (String[] args) {
		
		ByteBuf buff = Unpooled.wrappedBuffer("noreply".getBytes());
		byte[] out  = new byte[buff.readableBytes()];
		buff.readBytes(out);
		String in   = new String(out);
		System.out.println("String: " + in);
	}
}
