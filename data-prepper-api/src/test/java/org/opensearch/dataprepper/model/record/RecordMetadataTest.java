/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.record;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class RecordMetadataTest {
    @Test
    void defaultMetadata_returns_the_same_instance_when_called_multiple_times() {
        assertThat(RecordMetadata.defaultMetadata(), sameInstance(RecordMetadata.defaultMetadata()));
    }
    @Test
    void getMetadataObject_on_defaultMetadata_is_expected_value() {
        final RecordMetadata objectUnderTest = RecordMetadata.defaultMetadata();
        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getMetadataObject(), notNullValue());
        assertThat(objectUnderTest.getMetadataObject().size(), equalTo(1));
        assertThat(objectUnderTest.getMetadataObject().get(RecordMetadata.RECORD_TYPE), equalTo("unknown"));
    }
}