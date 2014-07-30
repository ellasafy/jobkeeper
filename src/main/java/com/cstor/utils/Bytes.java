package com.cstor.utils;

import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Bytes {
	private static final Log LOG = LogFactory.getLog(Bytes.class);

	public static byte[] toBytes(String s) {
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.error("UTF-8 not supported?", e);
			return null;
		}
	}

	public static String toStringBinary(final byte[] b) {
		if (b == null)
			return "null";
		return toStringBinary(b, 0, b.length);
	}

	public static String toStringBinary(final byte[] b, int off, int len) {
		StringBuilder result = new StringBuilder();
		try {
			String first = new String(b, off, len, "ISO-8859-1");
			for (int i = 0; i < first.length(); ++i) {
				int ch = first.charAt(i) & 0xFF;
				if ((ch >= '0' && ch <= '9')
						|| (ch >= 'A' && ch <= 'Z')
						|| (ch >= 'a' && ch <= 'z')
						|| " `~!@#$%^&*()-_=+[]{}\\|;:'\",.<>/?".indexOf(ch) >= 0) {
					result.append(first.charAt(i));
				} else {
					result.append(String.format("\\x%02X", ch));
				}
			}
		} catch (UnsupportedEncodingException e) {
			LOG.error("ISO-8859-1 not supported?", e);
		}
		return result.toString();
	}

	public static int compareTo(final byte[] left, final byte[] right) {
		return compareTo(left, 0, left.length, right, 0, right.length);
	}

	private static int compareTo(byte[] buffer1, int offset1, int length1,
			byte[] buffer2, int offset2, int length2) {
		// Short circuit equal case
		if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
			return 0;
		}
		// Bring WritableComparator code local
		int end1 = offset1 + length1;
		int end2 = offset2 + length2;
		for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
			int a = (buffer1[i] & 0xff);
			int b = (buffer2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return length1 - length2;
	}
}
