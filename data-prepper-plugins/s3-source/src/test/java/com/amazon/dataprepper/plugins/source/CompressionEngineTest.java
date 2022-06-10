/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CompressionEngineTest {
    private CompressionEngine compressionEngine;
    private ResponseInputStream<GetObjectResponse> responseInputStream;
    private String s3Key;

    @BeforeEach
    void setUp() {
        s3Key = UUID.randomUUID().toString();
        responseInputStream = mock(ResponseInputStream.class);
    }

    @Test
    void createInputStream_with_none_should_return_instance_of_ResponseInputStream() throws IOException {
        compressionEngine = new CompressionEngine(CompressionOption.NONE);
        InputStream inputStream = compressionEngine.createInputStream(s3Key, responseInputStream);
        assertThat(inputStream, sameInstance(responseInputStream));
        verifyNoInteractions(responseInputStream);
    }

    @Test
    void createInputStream_with_gzip_should_return_instance_of_GZIPInputStream() throws IOException {
        compressionEngine = new CompressionEngine(CompressionOption.GZIP);

        final String testString = UUID.randomUUID().toString();
        final byte[] testStringBytes = testString.getBytes(StandardCharsets.UTF_8);

        final ByteArrayInputStream byteInStream = getByteArrayInputStream(testStringBytes);

        InputStream inputStream = compressionEngine.createInputStream(s3Key, byteInStream);
        assertThat(inputStream, instanceOf(GZIPInputStream.class));
        assertThat(inputStream.readAllBytes(), equalTo(testStringBytes));
    }

    @Test
    void createInputStream_with_automatic_and_uncompressed_should_return_instance_of_ResponseInputStream() throws IOException {
        compressionEngine = new CompressionEngine(CompressionOption.AUTOMATIC);
        when(responseInputStream.response()).thenReturn(mock(GetObjectResponse.class));

        InputStream inputStream = compressionEngine.createInputStream(s3Key, responseInputStream);
        assertThat(inputStream, sameInstance(responseInputStream));
        verifyNoInteractions(responseInputStream);
    }

    @Test
    void createInputStream_with_automatic_and_compressed_should_return_instance_of_GZIPInputStream() throws IOException {
        compressionEngine = new CompressionEngine(CompressionOption.AUTOMATIC);
        s3Key = s3Key.concat(".gz");

        final String testString = UUID.randomUUID().toString();
        final byte[] testStringBytes = testString.getBytes(StandardCharsets.UTF_8);

        final ByteArrayInputStream byteInStream = getByteArrayInputStream(testStringBytes);

        InputStream inputStream = compressionEngine.createInputStream(s3Key, byteInStream);
        assertThat(inputStream, instanceOf(GZIPInputStream.class));
        assertThat(inputStream.readAllBytes(), equalTo(testStringBytes));
    }

    private ByteArrayInputStream getByteArrayInputStream(byte[] testStringBytes) throws IOException {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut);
        gzipOut.write(testStringBytes, 0, testStringBytes.length);
        gzipOut.close();
        final byte[] bites = byteOut.toByteArray();
        return new ByteArrayInputStream(bites);
    }
}