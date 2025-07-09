/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;


import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class CompressionOptionTest {
    @ParameterizedTest
    @EnumSource(CompressionOption.class)
    void fromOptionValue(final CompressionOption option) {
        assertThat(CompressionOption.fromOptionValue(option.name()), is(option));
        assertThat(option.getDecompressionEngine(), instanceOf(DecompressionEngine.class));
    }

    @Test
    void testFromFileName_gzip() {
        assertThat(CompressionOption.fromFileName("temp.gz"), is(CompressionOption.GZIP));
    }

    @Test
    void testFromFileName_snappy() {
        assertThat(CompressionOption.fromFileName("temp.snappy"), is(CompressionOption.SNAPPY));
    }

    @Test
    void testFromFileName_default() {
        assertThat(CompressionOption.fromFileName("temp.txt"), is(CompressionOption.NONE));
    }



    @ParameterizedTest
    @EnumSource(value = CompressionOption.class, names = {"AUTOMATIC"}, mode = EnumSource.Mode.EXCLUDE)
    void testCompressionDecompression(final CompressionOption option) throws Exception {
        String testData = "This is test data for compression and decompression";
        byte[] originalBytes = testData.getBytes();

        java.io.ByteArrayOutputStream compressedOutput = new java.io.ByteArrayOutputStream();
        java.io.OutputStream compressor = option.getCompressionEngine().createOutputStream(compressedOutput);
        compressor.write(originalBytes);
        compressor.close();

        byte[] compressedBytes = compressedOutput.toByteArray();
        java.io.InputStream decompressor = option.getDecompressionEngine().createInputStream(new java.io.ByteArrayInputStream(compressedBytes));
        byte[] decompressedBytes = decompressor.readAllBytes();

        assertThat(decompressedBytes, is(originalBytes));
    }






}