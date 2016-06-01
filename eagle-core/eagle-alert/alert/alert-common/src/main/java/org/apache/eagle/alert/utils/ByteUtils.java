package org.apache.eagle.alert.utils;

import java.io.UnsupportedEncodingException;

public class ByteUtils {

	public static double bytesToDouble(byte[] bytes, int offset){
		return Double.longBitsToDouble(bytesToLong(bytes, offset));
	}
	
	public static double bytesToDouble(byte[] bytes){
		return Double.longBitsToDouble(bytesToLong(bytes));
	}
	
	public static void doubleToBytes(double v, byte[] bytes){
		doubleToBytes(v, bytes, 0);
	}
	
	public static void doubleToBytes(double v, byte[] bytes, int offset){
		longToBytes(Double.doubleToLongBits(v), bytes, offset);
	}
	
	public static byte[] doubleToBytes(double v){
		return longToBytes(Double.doubleToLongBits(v));
	}
	
	public static long bytesToLong(byte[] bytes){
		return bytesToLong(bytes, 0);
	}
	
	public static long bytesToLong(byte[] bytes, int offset){
		long value = 0;
		for(int i=0; i<8; i++){
			value <<= 8;
			value |= (bytes[i+offset] & 0xFF);
		}
		return value;
	}
	
	public static void longToBytes(long v, byte[] bytes){
		longToBytes(v, bytes, 0);
	}
	
	public static void longToBytes(long v, byte[] bytes, int offset){
		long tmp = v;
		for(int i=0; i<8; i++){
			bytes[offset + 7 - i] = (byte)(tmp & 0xFF);
			tmp >>= 8;
		}
	}
	
	public static byte[] longToBytes(long v){
		long tmp = v;
		byte[] b = new byte[8];
		for(int i=0; i<8; i++){
			b[7-i] = (byte)(tmp & 0xFF);
			tmp >>= 8;
		}
		return b;
	}
	
	public static int bytesToInt(byte[] bytes){
		return bytesToInt(bytes, 0);
	}
	
	public static int bytesToInt(byte[] bytes, int offset){
		int value = 0;
		for(int i=0; i<4; i++){
			value <<= 8;
			value |= (bytes[i+offset] & 0xFF);
		}
		return value;
	}
	
	public static void intToBytes(int v, byte[] bytes){
		intToBytes(v, bytes, 0);
	}
	
	public static void intToBytes(int v, byte[] bytes, int offset){
		int tmp = v;
		for(int i=0; i<4; i++){
			bytes[offset + 3 - i] = (byte)(tmp & 0xFF);
			tmp >>= 8;
		}
	}

	public static byte[] intToBytes(int v){
		int tmp = v;
		byte[] b = new byte[4];
		for(int i=0; i<4; i++){
			b[3-i] = (byte)(tmp & 0xFF);
			tmp >>= 8;
		}
		return b;
	}

	//////
	
	public static short bytesToShort(byte[] bytes){
		return bytesToShort(bytes, 0);
	}
	
	public static short bytesToShort(byte[] bytes, int offset){
		short value = 0;
		for(int i=0; i < 2; i++){
			value <<= 8;
			value |= (bytes[i+offset] & 0xFF);
		}
		return value;
	}
	
	public static void shortToBytes(short v, byte[] bytes){
		shortToBytes(v, bytes, 0);
	}
	
	public static void shortToBytes(short v, byte[] bytes, int offset){
		int tmp = v;
		for(int i=0; i < 2; i++){
			bytes[offset + 1 - i] = (byte)(tmp & 0xFF);
			tmp >>= 8;
		}
	}

	public static byte[] shortToBytes(short v){
		int tmp = v;
		byte[] b = new byte[2];
		for(int i=0; i<2; i++){
			b[1-i] = (byte)(tmp & 0xFF);
			tmp >>= 8;
		}
		return b;
	}

	public static byte[] concat(byte[]... arrays) {
        int length = 0;
        for (byte[] array : arrays) {
            length += array.length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, pos, array.length);
            pos += array.length;
        }
        return result;
    }

	public static byte[] stringToBytes(String str) {
		try {
			return str.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}
	}

//    public static void main(String[] args){ 
//    	int a = "ThreadName".hashCode();
//    	byte[] b = intToBytes(a);
//    	byte[] c = intToBytes(1676687583);
//    	String s = new String(b);
//    	System.out.println(s);
    	
//    	byte[] d = intToBytes(8652353);
//    	System.out.println(bytesToInt(d));
    	
//    	byte[] e = longToBytes(12131513513l);
//    	System.out.println(bytesToLong(e));
//    	if(12131513513l == bytesToLong(e)){
//    		System.out.println("yes");
//    	}
//    }
}