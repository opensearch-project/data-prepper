package com.amazon.situp.plugins.processor.oteltrace.model;

import java.util.Random;

public class TestUtils {

    private static final Random RANDOM =  new Random();

    public static byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

}
