/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
