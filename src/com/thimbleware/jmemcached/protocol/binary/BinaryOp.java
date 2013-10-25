package com.thimbleware.jmemcached.protocol.binary;

import com.thimbleware.jmemcached.protocol.CommandMessage;
import com.thimbleware.jmemcached.protocol.Op;

public enum BinaryOp {
	Get(0x00, Op.GET, false),
	Set(0x01, Op.SET, false),
	Add(0x02, Op.ADD, false),
	Replace(0x03, Op.REPLACE, false), 
	Delete(0x04, Op.DELETE, false), 
	Increment(0x05, Op.INCR, false), 
	Decrement(0x06, Op.DECR, false), 
	Quit(0x07, Op.QUIT, false), 
	Flush(0x08, Op.FLUSH_ALL, false), 
	GetQ(0x09, Op.GET, false), 
	Noop(0x0A, null, false), 
	Version(0x0B, Op.VERSION, false), 
	GetK(0x0C, Op.GET, false, true), 
	GetKQ(0x0D, Op.GET, true, true),
	Append(0x0E, Op.APPEND, false),
	Prepend(0x0F, Op.PREPEND, false),
	Stat(0x10, Op.STATS, false),
	SetQ(0x11, Op.SET, true), 
	AddQ(0x12, Op.ADD, true), 
	ReplaceQ(0x13, Op.REPLACE, true), 
	DeleteQ(0x14, Op.DELETE, true), 
	IncrementQ(0x15, Op.INCR, true), 
	DecrementQ(0x16, Op.DECR, true), 
	QuitQ(0x17, Op.QUIT, true), 
	FlushQ(0x18, Op.FLUSH_ALL, true), 
	AppendQ(0x19, Op.APPEND, true), 
	PrependQ(0x1A, Op.PREPEND, true);

	public byte code;
	public Op correspondingOp;
	public boolean noreply;
	public boolean addKeyToResponse = false;

	BinaryOp(int code, Op correspondingOp, boolean noreply) {
		this.code = (byte) code;
		this.correspondingOp = correspondingOp;
		this.noreply = noreply;
	}

	BinaryOp(int code, Op correspondingOp, boolean noreply,
			boolean addKeyToResponse) {
		this.code = (byte) code;
		this.correspondingOp = correspondingOp;
		this.noreply = noreply;
		this.addKeyToResponse = addKeyToResponse;
	}

	public static BinaryOp forCommandMessage(CommandMessage msg) {
		for (BinaryOp binaryOp : values()) {
			if (binaryOp.correspondingOp == msg.op
					&& binaryOp.noreply == msg.noreply
					&& binaryOp.addKeyToResponse == msg.addKeyToResponse) {
				return binaryOp;
			}
		}

		return null;
	}

}
