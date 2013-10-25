package com.thimbleware.jmemcached.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 */
public class BufferUtils {

    final static int [] sizeTable = { 9, 99, 999, 9999, 99999, 999999, 9999999,
            99999999, 999999999, Integer.MAX_VALUE };
    private static final ByteBuf LONG_MIN_VALUE_BYTES = Unpooled.wrappedBuffer("-9223372036854775808".getBytes());

    // Requires positive x
    static int stringSize(int x) {
        for (int i=0; ; i++)
            if (x <= sizeTable[i])
                return i+1;
    }

    final static byte[] digits = {
            '0' , '1' , '2' , '3' , '4' , '5' ,
            '6' , '7' , '8' , '9' , 'a' , 'b' ,
            'c' , 'd' , 'e' , 'f' , 'g' , 'h' ,
            'i' , 'j' , 'k' , 'l' , 'm' , 'n' ,
            'o' , 'p' , 'q' , 'r' , 's' , 't' ,
            'u' , 'v' , 'w' , 'x' , 'y' , 'z'
    };

    final static byte [] DigitTens = {
            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
            '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
            '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
            '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
            '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
            '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
            '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
            '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
            '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
            '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
    } ;

    final static byte [] DigitOnes = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    } ;


    public static int atoi(ByteBuf s)
            throws NumberFormatException
    {
        int result = 0;
        boolean negative = false;
        int i = 0, len = s.capacity();
        int limit = -Integer.MAX_VALUE;
        int multmin;
        int digit;

        if (len > 0) {
            byte firstChar = s.getByte(0);
            if (firstChar < '0') { // Possible leading "-"
                if (firstChar == '-') {
                    negative = true;
                    limit = Integer.MIN_VALUE;
                } else
                    throw new NumberFormatException();

                if (len == 1) // Cannot have lone "-"
                    throw new NumberFormatException();
                i++;
            }
            multmin = limit / 10;
            while (i < len) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = Character.digit(s.getByte(i++),10);
                if (digit < 0) {
                    throw new NumberFormatException();
                }
                if (result < multmin) {
                    throw new NumberFormatException();
                }
                result *= 10;
                if (result < limit + digit) {
                    throw new NumberFormatException();
                }
                result -= digit;
            }
        } else {
            throw new NumberFormatException();
        }
        return negative ? result : -result;
    }

    public static long atol(ByteBuf s)
            throws NumberFormatException
    {
        long result = 0;
        boolean negative = false;
        int i = 0, len = s.capacity();
        long limit = -Long.MAX_VALUE;
        long multmin;
        int digit;

        if (len > 0) {
            byte firstChar = s.getByte(0);
            if (firstChar < '0') { // Possible leading "-"
                if (firstChar == '-') {
                    negative = true;
                    limit = Long.MIN_VALUE;
                } else
                    throw new NumberFormatException();

                if (len == 1) // Cannot have lone "-"
                    throw new NumberFormatException();
                i++;
            }
            multmin = limit / 10;
            while (i < len) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = Character.digit(s.getByte(i++),10);
                if (digit < 0) {
                    throw new NumberFormatException();
                }
                if (result < multmin) {
                    throw new NumberFormatException();
                }
                result *= 10;
                if (result < limit + digit) {
                    throw new NumberFormatException();
                }
                result -= digit;
            }
        } else {
            throw new NumberFormatException();
        }
        return negative ? result : -result;
    }

    /** Blatant copy of Integer.toString, but returning a byte array instead of a String, as
     *  string charset decoding/encoding was killing us on performance.
     * @param i integer to convert
     * @return byte[] array containing literal ASCII char representation
     */
    public static ByteBuf itoa(int i) {
        int size = (i < 0) ? stringSize(-i) + 1 : stringSize(i);
        ByteBuf buf = Unpooled.buffer(size);
        getChars(i, size, buf);
        return buf;
    }


    public static ByteBuf ltoa(long i) {
        if (i == Long.MIN_VALUE)
            return LONG_MIN_VALUE_BYTES;
        int size = (i < 0) ? stringSize(-i) + 1 : stringSize(i);
        ByteBuf buf = Unpooled.buffer(size);
        getChars(i, size, buf);
        return buf;
    }

    /**
     * Places characters representing the integer i into the
     * character array buf. The characters are placed into
     * the buffer backwards starting with the least significant
     * digit at the specified index (exclusive), and working
     * backwards from there.
     *
     * Will fail if i == Long.MIN_VALUE
     */
    static void getChars(long i, int index, ByteBuf buf) {
        long q;
        int r;
        int charPos = index;
        byte sign = 0;

        if (i < 0) {
            sign = '-';
            i = -i;
        }

        // Get 2 digits/iteration using longs until quotient fits into an int
        while (i > Integer.MAX_VALUE) {
            q = i / 100;
            // really: r = i - (q * 100);
            r = (int)(i - ((q << 6) + (q << 5) + (q << 2)));
            i = q;
            buf.setByte(--charPos, DigitOnes[r]);
            buf.setByte(--charPos, DigitTens[r]);
        }

        // Get 2 digits/iteration using ints
        int q2;
        int i2 = (int)i;
        while (i2 >= 65536) {
            q2 = i2 / 100;
            // really: r = i2 - (q * 100);
            r = i2 - ((q2 << 6) + (q2 << 5) + (q2 << 2));
            i2 = q2;
            buf.setByte(--charPos, DigitOnes[r]);
            buf.setByte(--charPos, DigitTens[r]);
        }

        // Fall thru to fast mode for smaller numbers
        // assert(i2 <= 65536, i2);
        for (;;) {
            q2 = (i2 * 52429) >>> (16+3);
            r = i2 - ((q2 << 3) + (q2 << 1));  // r = i2-(q2*10) ...
            buf.setByte(--charPos, digits[r]);
            i2 = q2;
            if (i2 == 0) break;
        }
        if (sign != 0) {
            buf.setByte(--charPos, sign);
        }
        buf.writerIndex(buf.capacity());
    }

    static void getChars(int i, int index, ByteBuf buf) {
        int q, r;
        int charPos = index;
        byte sign = 0;

        if (i < 0) {
            sign = '-';
            i = -i;
        }

        // Generate two digits per iteration
        while (i >= 65536) {
            q = i / 100;
            // really: r = i - (q * 100);
            r = i - ((q << 6) + (q << 5) + (q << 2));
            i = q;
            buf.setByte(--charPos, DigitOnes[r]);
            buf.setByte(--charPos, DigitTens[r]);
        }

        // Fall thru to fast mode for smaller numbers
        // assert(i <= 65536, i);
        for (;;) {
            q = (i * 52429) >>> (16+3);
            r = i - ((q << 3) + (q << 1));  // r = i-(q*10) ...
            buf.setByte(--charPos, digits[r]);
            i = q;
            if (i == 0) break;
        }
        if (sign != 0) {
            buf.setByte(--charPos, sign);
        }
        buf.writerIndex(buf.capacity());
    }

    // Requires positive x
    static int stringSize(long x) {
        long p = 10;
        for (int i=1; i<19; i++) {
            if (x < p)
                return i;
            p = 10*p;
        }
        return 19;
    }

    public static String ByteBufToHex (ByteBuf in) {
    	in = in.copy();
    	in.resetReaderIndex();
    	final char[] hexArray = "0123456789abcdef".toCharArray();
    	byte[] inBytes = new byte[in.capacity()];
    	in.getBytes(0, inBytes);
    	
    	char[] out = new char[inBytes.length * 2];
    	
    	int v, t, j = 0;

    	final int l = inBytes.length;
    	
    	for (; j != l; j++) {
    		v = (inBytes[j] & 0xFF);
    		t = j << 1;
    		
    		out[t    ] = hexArray[v >>> 4];
    		out[t + 1] = hexArray[v & 0x0F];
    	}
    	
    	return new String(out);
    }
    
    public static String ByteBufToStr (ByteBuf in) {
    	String out = in.toString(0, in.capacity(), Charset.defaultCharset());
    	
    	out = out.replace('\r', '#');
    	out = out.replace('\n', '&');
    	
    	return out;
    }
    
}
