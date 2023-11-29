/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

