/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.KeyGenerator;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3GroupIdentifierFactoryTest {

    private KeyGenerator keyGenerator;

    @BeforeEach
    void setup() {
        keyGenerator = mock(KeyGenerator.class);
    }

    private S3GroupIdentifierFactory createObjectUnderTest() {
        return new S3GroupIdentifierFactory(keyGenerator);
    }

    @Test
    void getS3GroupIdentifierForEvent_returns_expected_s3GroupIdentifier() {
        final String expectedIdentificationHash = UUID.randomUUID().toString();
        final String expectedFullObjectKey = UUID.randomUUID().toString();
        final Event event = mock(Event.class);

        when(keyGenerator.generateKeyForEvent(event, false)).thenReturn(expectedIdentificationHash);
        when(keyGenerator.generateKeyForEvent(event, true)).thenReturn(expectedFullObjectKey);

        final S3GroupIdentifierFactory objectUnderTest = createObjectUnderTest();

        final S3GroupIdentifier result = objectUnderTest.getS3GroupIdentifierForEvent(event);

        assertThat(result, notNullValue());
        assertThat(result.getGroupIdentifierFullObjectKey(), equalTo(expectedFullObjectKey));
        assertThat(result.getGroupIdentifierHash(), equalTo(expectedIdentificationHash));
    }
}
