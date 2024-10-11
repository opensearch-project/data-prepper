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
    private ObjectMetadataConfig objectMetadataConfig;
    @Mock
    private PredefinedObjectMetadata predefinedObjectMetadata;
    private String numberOfEventsKey;

    private ObjectMetadata createObjectUnderTest(Object metadataConfig) {
        return new ObjectMetadata(metadataConfig);
    }

    @BeforeEach
    public void setup() {
        objectMetadataConfig = mock(ObjectMetadataConfig.class);
        predefinedObjectMetadata = mock(PredefinedObjectMetadata.class);
        numberOfEventsKey = UUID.randomUUID().toString();
        when(objectMetadataConfig.getNumberOfEventsKey()).thenReturn(numberOfEventsKey);
        when(predefinedObjectMetadata.getNumberOfObjects()).thenReturn(numberOfEventsKey);
    }

    @Test
    public void test_setEventCount_with_PredefinedObjectMetadata() {
        objectMetadata = createObjectUnderTest(predefinedObjectMetadata);
        Random random = new Random();
        Integer numEvents = Math.abs(random.nextInt());
        objectMetadata.setEventCount(numEvents);
        assertThat(objectMetadata.get().get(numberOfEventsKey), equalTo(Integer.toString(numEvents)));
    }

    @Test
    void test_setEventCount_with_ObjectMetadata() {
        objectMetadata = createObjectUnderTest(objectMetadataConfig);
        Random random = new Random();
	    Integer numEvents = Math.abs(random.nextInt());
        objectMetadata.setEventCount(numEvents);
        assertThat(objectMetadata.get().get(numberOfEventsKey), equalTo(Integer.toString(numEvents)));
    }

}
