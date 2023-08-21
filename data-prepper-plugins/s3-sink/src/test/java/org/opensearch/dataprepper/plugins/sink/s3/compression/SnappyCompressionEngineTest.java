/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.compression;

import org.junit.jupiter.api.Test;
import org.xerial.snappy.SnappyCodec;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

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

class SnappyCompressionEngineTest {

    private SnappyCompressionEngine createObjectUnderTest() {
        return new SnappyCompressionEngine();
    }

    @Test
    void createOutputStream_should_return_SnappyOutputStream() throws IOException {
        final OutputStream innerOutputStream = mock(OutputStream.class);
        final OutputStream outputStream = createObjectUnderTest().createOutputStream(innerOutputStream);

        assertThat(outputStream, instanceOf(SnappyOutputStream.class));
    }

    @Test
    void createOutputStream_should_write_compressed_data() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final OutputStream outputStream = createObjectUnderTest().createOutputStream(byteArrayOutputStream);

        final byte[] inputBytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

        outputStream.write(inputBytes);
        outputStream.close();

        final byte[] writtenBytes = byteArrayOutputStream.toByteArray();

        assertTrue(SnappyCodec.hasMagicHeaderPrefix(writtenBytes));

        final ByteArrayInputStream verificationInputStream = new ByteArrayInputStream(writtenBytes);

        final SnappyInputStream uncompressingInputStream = new SnappyInputStream(verificationInputStream);
        final byte[] uncompressedBytes = uncompressingInputStream.readAllBytes();
        assertThat(uncompressedBytes, equalTo(inputBytes));
    }
}