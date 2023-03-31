/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.compression;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AutomaticCompressionEngineTest {
    private CompressionEngine compressionEngine;
    private ResponseInputStream<GetObjectResponse> responseInputStream;
    private String s3Key;

    @BeforeEach
    void setUp() {
        s3Key = UUID.randomUUID().toString();
        responseInputStream = mock(ResponseInputStream.class);
    }

    private AutomaticCompressionEngine createObjectUnderTest() {
        return new AutomaticCompressionEngine();
    }

    @Test
    void createInputStream_with_automatic_and_uncompressed_should_return_instance_of_ResponseInputStream() throws IOException {
        when(responseInputStream.response()).thenReturn(mock(GetObjectResponse.class));

        final InputStream inputStream = createObjectUnderTest().createInputStream(s3Key, responseInputStream);
        assertThat(inputStream, sameInstance(responseInputStream));
        verifyNoInteractions(responseInputStream);
    }

    @Test
    void createInputStream_with_automatic_and_compressed_should_return_instance_of_GZIPInputStream() throws IOException {
        compressionEngine = createObjectUnderTest();
        s3Key = s3Key.concat(".gz");

        final String testString = UUID.randomUUID().toString();
        final byte[] testStringBytes = testString.getBytes(StandardCharsets.UTF_8);

        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut);
        gzipOut.write(testStringBytes, 0, testStringBytes.length);
        gzipOut.close();
        final byte[] bites = byteOut.toByteArray();
        final ByteArrayInputStream byteInStream = new ByteArrayInputStream(bites);

        final InputStream inputStream = compressionEngine.createInputStream(s3Key, byteInStream);

        assertThat(inputStream, instanceOf(GzipCompressorInputStream.class));
        assertThat(inputStream.readAllBytes(), equalTo(testStringBytes));
    }

    @ParameterizedTest
    @ValueSource(strings = { ".snappy", ".snappy.parquet"})
    void createInputStream_with_automatic_and_compressed_should_return_instance_of_SnappyInputStream(final String fileExtension) throws IOException {

        s3Key.concat(fileExtension);
        compressionEngine = new SnappyCompressionEngine();
        final String testString = UUID.randomUUID().toString();
        final byte[] testStringBytes = testString.getBytes(StandardCharsets.UTF_8);
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final SnappyOutputStream snappyOut = new SnappyOutputStream(byteOut);

        snappyOut.write(testStringBytes);
        snappyOut.close();

        final byte[] bites = byteOut.toByteArray();
        final ByteArrayInputStream byteInStream = new ByteArrayInputStream(bites);
        final InputStream inputStream = compressionEngine.createInputStream(s3Key, byteInStream);

        assertThat(inputStream, instanceOf(SnappyInputStream.class));
        assertThat(inputStream.readAllBytes(),equalTo(testStringBytes));
    }
}
