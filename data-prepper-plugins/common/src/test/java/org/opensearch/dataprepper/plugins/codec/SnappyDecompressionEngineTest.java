/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

class SnappyDecompressionEngineTest {
    private DecompressionEngine decompressionEngine;
    private ResponseInputStream<GetObjectResponse> responseInputStream;
    private String s3Key;

    @BeforeEach
    void setUp() {
        s3Key = UUID.randomUUID().toString();
        responseInputStream = mock(ResponseInputStream.class);
    }

    @Test
    void createInputStream_with_snappy_should_return_instance_of_SnappyInputStream() throws IOException {

        s3Key.concat(".snappy.parquet");
        decompressionEngine = new SnappyDecompressionEngine();
        final String testString = UUID.randomUUID().toString();
        final byte[] testStringBytes = testString.getBytes(StandardCharsets.UTF_8);
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final SnappyOutputStream snappyOut = new SnappyOutputStream(byteOut);

        snappyOut.write(testStringBytes);
        snappyOut.close();

        final byte[] bites = byteOut.toByteArray();
        final ByteArrayInputStream byteInStream = new ByteArrayInputStream(bites);
        final InputStream inputStream = decompressionEngine.createInputStream(byteInStream);

        assertThat(inputStream, instanceOf(SnappyInputStream.class));
        assertThat(inputStream.readAllBytes(),equalTo(testStringBytes));

    }
}
