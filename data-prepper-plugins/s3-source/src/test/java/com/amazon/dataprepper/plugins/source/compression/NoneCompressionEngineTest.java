/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.compression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class NoneCompressionEngineTest {
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
        compressionEngine = new NoneCompressionEngine();
        final InputStream inputStream = compressionEngine.createInputStream(s3Key, responseInputStream);

        assertThat(inputStream, sameInstance(responseInputStream));
        verifyNoInteractions(responseInputStream);
    }
}
