/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.compression;

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
import static org.mockito.Mockito.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class GZipCompressionEngineTest {
    private CompressionEngine compressionEngine;
    private ResponseInputStream<GetObjectResponse> responseInputStream;
    private String s3Key;

    @BeforeEach
    void setUp() {
        s3Key = UUID.randomUUID().toString();
        responseInputStream = mock(ResponseInputStream.class);
    }

    @Test
    void createInputStream_with_gzip_should_return_instance_of_GZIPInputStream() throws IOException {
        compressionEngine = new GZipCompressionEngine();

        final String testString = UUID.randomUUID().toString();
        final byte[] testStringBytes = testString.getBytes(StandardCharsets.UTF_8);

        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut);
        gzipOut.write(testStringBytes, 0, testStringBytes.length);
        gzipOut.close();
        final byte[] bites = byteOut.toByteArray();
        final ByteArrayInputStream byteInStream = new ByteArrayInputStream(bites);

        final InputStream inputStream = compressionEngine.createInputStream(s3Key, byteInStream);

        assertThat(inputStream, instanceOf(GZIPInputStream.class));
        assertThat(inputStream.readAllBytes(), equalTo(testStringBytes));
    }
}
