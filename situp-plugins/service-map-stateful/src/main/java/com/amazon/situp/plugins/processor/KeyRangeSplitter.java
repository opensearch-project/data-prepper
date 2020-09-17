package com.amazon.situp.plugins.processor;

import java.math.BigInteger;
import java.nio.charset.Charset;

public class KeyRangeSplitter {
    private final BigInteger lowEnd;
    private final BigInteger highEnd;
    private final Charset charset;
    private final int maxLen;

    public KeyRangeSplitter(final String lowEnd, final String highEnd, final Charset charset, final int maxLen) {
        this.maxLen = maxLen;
        this.charset = charset;
        this.lowEnd = new BigInteger(getMaxLenByteArray(lowEnd).getBytes(charset));
        this.highEnd = new BigInteger(getMaxLenByteArray(highEnd).getBytes(charset));
    }

    private String getMaxLenByteArray(final String s) {
        final byte[] bytes = new byte[maxLen];
        final byte[] sBytes = s.getBytes(charset);
        for(int i = 0; i < sBytes.length; i++) {
            bytes[i] = sBytes[i];
        }
        return new String(bytes, charset);
    }

    public String getBoundary(final long index, final long segments) {
        final BigInteger step = highEnd.subtract(lowEnd).divide(BigInteger.valueOf(segments));
        final BigInteger bigIntVal = lowEnd.add(BigInteger.valueOf(index).multiply(step));
        return new String(bigIntVal.toByteArray(), charset);
    }

}
