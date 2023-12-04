/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.ion;

import com.amazon.ion.Timestamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IonTimestampConverterModuleTest {
    @Test void test_when_module_is_installed_then_returns_string_representation_of_timestamp() throws JsonProcessingException {
        final IonObjectMapper objectMapper = new IonObjectMapper();
        objectMapper.registerModule(new IonTimestampConverterModule());

        final String timestamp = "2023-11-30T21:05:23.383Z";
        final String expectedValue = "\"" + timestamp + "\"";
        final String actualValue = objectMapper.writeValueAsString(Timestamp.valueOf(timestamp));

        assertEquals(expectedValue, actualValue);
    }
}