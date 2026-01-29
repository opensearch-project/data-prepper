/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class ResponseHandlingTest {

    @Test
    public void testFromOptionNameWithValidValue() {
        assertEquals(ResponseHandling.RECONSTRUCT_DOCUMENT, ResponseHandling.fromOptionName("reconstruct-document"));
    }

    @Test
    public void testGetOptionName() {
        assertEquals("reconstruct-document", ResponseHandling.RECONSTRUCT_DOCUMENT.getOptionName());
    }
}
