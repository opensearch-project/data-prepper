/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.UUID;

public class ObjectMetadataConfigTest {
    @Test
    void test_default() {
        assertThat(new ObjectMetadataConfig().getNumberOfEventsKey(), equalTo(null));
    }

    @Test
    void test_number_of_events_key() throws Exception {
        final String numberOfEventsKey = UUID.randomUUID().toString();
        final ObjectMetadataConfig objectUnderTest = new ObjectMetadataConfig();
        ReflectivelySetField.setField(ObjectMetadataConfig.class, objectUnderTest, "numberOfEventsKey", numberOfEventsKey);
        assertThat(objectUnderTest.getNumberOfEventsKey(), equalTo(numberOfEventsKey));
    }
}

