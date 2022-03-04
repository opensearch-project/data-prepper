/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class GrokMatchUtilTest {

    @ParameterizedTest
    @CsvSource({"%{NUMBER}, %{NUMBER}", "%{GREEDYDATA:field}, %{GREEDYDATA:field}"})
    void convertGrokMatchPattern_without_nested_fields_should_return_same_string_Test(String matchPattern, String convertedMatchPattern) {
        String convertedString = GrokMatchUtil.convertGrokMatchPattern(matchPattern);
        assertThat(convertedString, equalTo(convertedMatchPattern));
    }

    @ParameterizedTest
    @CsvSource({"%{NUMBER} %{GREEDYDATA:[nested][field][data]}, %{NUMBER} %{GREEDYDATA:/nested/field/data}", "%{NUMBER:[field]}, %{NUMBER:/field}"})
    void convertGrokMatchPattern_with_nested_fields_should_return_converted_string_Test(String matchPattern, String convertedMatchPattern) {
        String convertedString = GrokMatchUtil.convertGrokMatchPattern(matchPattern);
        assertThat(convertedString, equalTo(convertedMatchPattern));
    }

    @ParameterizedTest
    @CsvSource({"%{NUMBER:[nested][field][data]:int}, %{NUMBER:/nested/field/data:int}", "%{NUMBER:count:int}, %{NUMBER:count:int}"})
    void convertGrokMatchPattern_with_nested_fields_including_data_type_should_return_converted_string_Test(String matchPattern, String convertedMatchPattern) {
        String convertedString = GrokMatchUtil.convertGrokMatchPattern(matchPattern);
        assertThat(convertedString, equalTo(convertedMatchPattern));
    }
}