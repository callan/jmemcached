package com.thimbleware.jmemcached.protocol.text;

import io.netty.buffer.ByteBuf;

import java.util.List;

public class SplitList {

	private List<ByteBuf> list;
	private boolean eof;
	
	public SplitList(List<ByteBuf> list, boolean eof) {
		this.list = list;
		this.eof  = eof;
	}
	
	public boolean hasEof () {
		return eof;
	}
	
	public List<ByteBuf> getList () {
		return list;
	}
}
