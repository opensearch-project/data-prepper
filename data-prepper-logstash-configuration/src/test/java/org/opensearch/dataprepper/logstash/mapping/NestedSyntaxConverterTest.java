/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class NestedSyntaxConverterTest {

    @ParameterizedTest
    @CsvSource({"[message], /message", "[nested][field], /nested/field"})
    void convertNestedSyntaxToJsonPath_with_nested_field_should_return_json_pointer_test(String nestedField, String expectedJsonPointer) {
        String actualJsonPointer = NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(nestedField);

        assertThat(actualJsonPointer, equalTo(expectedJsonPointer));
    }

    @ParameterizedTest
    @CsvSource({"message, message", "field, field"})
    void convertNestedSyntaxToJsonPath_with_no_nested_field_should_return_same_string_test(String nestedField, String expectedJsonPointer) {
        String actualJsonPointer = NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(nestedField);

        assertThat(actualJsonPointer, equalTo(expectedJsonPointer));
    }
}