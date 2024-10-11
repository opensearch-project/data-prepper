/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import static org.hamcrest.CoreMatchers.equalTo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Random;
import java.util.UUID;

public class ObjectMetadataTest {
    private ObjectMetadata objectMetadata;
    @Mock
    ObjectMetadataConfig objectMetadataConfig;
    private String numberOfEventsKey;

    private ObjectMetadata createObjectUnderTest() {
        return new ObjectMetadata(objectMetadataConfig);
    }

    @BeforeEach
    void setup() {
        objectMetadataConfig = mock(ObjectMetadataConfig.class);
        numberOfEventsKey = UUID.randomUUID().toString();
        when(objectMetadataConfig.getNumberOfEventsKey()).thenReturn(numberOfEventsKey);
        objectMetadata = createObjectUnderTest();
    }

    @Test
    void test_setEventCount() {
        Random random = new Random();
	Integer numEvents = random.nextInt();
        objectMetadata.setEventCount(numEvents);
        assertThat(objectMetadata.get().get(numberOfEventsKey), equalTo(Integer.toString(numEvents)));
    }

}
