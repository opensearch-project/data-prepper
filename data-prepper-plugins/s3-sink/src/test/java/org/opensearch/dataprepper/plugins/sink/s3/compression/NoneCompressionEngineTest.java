/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.compression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class NoneCompressionEngineTest {

    private OutputStream innerOutputStream;

    @BeforeEach
    void setUp() {
        innerOutputStream = mock(OutputStream.class);
    }

    private NoneCompressionEngine createObjectUnderTest() {
        return new NoneCompressionEngine();
    }

    @Test
    void createOutputStream_returns_innerOutputStream() {
        OutputStream outputStream = createObjectUnderTest().createOutputStream(innerOutputStream);

        assertThat(outputStream, sameInstance(innerOutputStream));
        verifyNoInteractions(innerOutputStream);
    }
}