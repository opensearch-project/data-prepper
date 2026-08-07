/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.codec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;

public class HasByteDecoderTest {

    @Test
    public void testGetDecoder() {
        final HasByteDecoder hasByteDecoder = spy(HasByteDecoder.class);

        assertEquals(null, hasByteDecoder.getDecoder());
    }

}

