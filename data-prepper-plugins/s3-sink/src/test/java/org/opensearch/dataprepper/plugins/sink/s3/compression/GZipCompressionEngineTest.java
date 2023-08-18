/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.compression;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class GZipCompressionEngineTest {

    private GZipCompressionEngine createObjectUnderTest() {
        return new GZipCompressionEngine();
    }

    @Test
    void createOutputStream_should_return_GzipCompressorOutputStream() throws IOException {
        final OutputStream innerOutputStream = mock(OutputStream.class);
        final OutputStream outputStream = createObjectUnderTest().createOutputStream(innerOutputStream);

        assertThat(outputStream, instanceOf(GzipCompressorOutputStream.class));
    }

    @Test
    void createOutputStream_should_write_compressed_data() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final OutputStream outputStream = createObjectUnderTest().createOutputStream(byteArrayOutputStream);

        final byte[] inputBytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

        outputStream.write(inputBytes);
        outputStream.close();

        final byte[] writtenBytes = byteArrayOutputStream.toByteArray();

        assertTrue(GzipCompressorInputStream.matches(writtenBytes, 2));

        final ByteArrayInputStream verificationInputStream = new ByteArrayInputStream(writtenBytes);

        final GzipCompressorInputStream uncompressingInputStream = new GzipCompressorInputStream(verificationInputStream);
        final byte[] uncompressedBytes = uncompressingInputStream.readAllBytes();
        assertThat(uncompressedBytes, equalTo(inputBytes));
    }
}