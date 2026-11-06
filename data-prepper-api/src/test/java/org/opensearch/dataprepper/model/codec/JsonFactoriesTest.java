/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.codec;

import com.fasterxml.jackson.core.JsonFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class JsonFactoriesTest {
    @Test
    void defaultJsonFactory_returns_factory_with_the_data_prepper_maximum_string_length() {
        final JsonFactory jsonFactory = JsonFactories.defaultJsonFactory();

        assertThat(jsonFactory.streamReadConstraints().getMaxStringLength(),
                equalTo(JsonFactories.MAX_JSON_STRING_LENGTH));
    }

    @Test
    void defaultJsonFactory_returns_a_new_factory_for_each_call() {
        assertThat(JsonFactories.defaultJsonFactory(), not(sameInstance(JsonFactories.defaultJsonFactory())));
    }

    @ParameterizedTest
    @ValueSource(ints = {1024, 20_000_000, 64 * 1024 * 1024})
    void jsonFactory_returns_factory_with_the_provided_maximum_string_length(final int maxStringLength) {
        final JsonFactory jsonFactory = JsonFactories.jsonFactory(maxStringLength);

        assertThat(jsonFactory.streamReadConstraints().getMaxStringLength(), equalTo(maxStringLength));
    }
}
