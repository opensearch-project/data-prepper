/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import org.apache.commons.text.similarity.LevenshteinDistance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.pipeline.parser.ClosestFieldRecommender.MIN_DISTANCE_TO_RECOMMEND_PROPERTY;

@ExtendWith(MockitoExtension.class)
class ClosestFieldRecommenderTest {
    @Mock
    private LevenshteinDistance levenshteinDistance;

    private ClosestFieldRecommender createObjectUnderTest() {
        return new ClosestFieldRecommender(levenshteinDistance);
    }

    @Test
    void testGetClosestFieldWithDistanceLargerThanMinDistanceToRecommend() {
        final List<Object> knownPropertyIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String propertyName = UUID.randomUUID().toString();
        when(levenshteinDistance.apply(eq(propertyName), anyString())).thenReturn(
                MIN_DISTANCE_TO_RECOMMEND_PROPERTY + 1);
        final ClosestFieldRecommender objectUnderTest = createObjectUnderTest();
        final Optional<String> closestFieldOptional = objectUnderTest.getClosestField(propertyName, knownPropertyIds);
        assertThat(closestFieldOptional.isEmpty(), is(true));
    }

    @Test
    void testGetClosestFieldWithDistanceWithinMinDistanceToRecommend() {
        final String testKnownPropertyId1 = UUID.randomUUID().toString();
        final String testKnownPropertyId2 = testKnownPropertyId1 + "2";
        final List<Object> knownPropertyIds = List.of(testKnownPropertyId1, testKnownPropertyId2);
        final String propertyName = UUID.randomUUID().toString();
        when(levenshteinDistance.apply(eq(propertyName), eq(testKnownPropertyId1))).thenReturn(
                MIN_DISTANCE_TO_RECOMMEND_PROPERTY - 1);
        when(levenshteinDistance.apply(eq(propertyName), eq(testKnownPropertyId2))).thenReturn(
                MIN_DISTANCE_TO_RECOMMEND_PROPERTY);
        final ClosestFieldRecommender objectUnderTest = createObjectUnderTest();
        final Optional<String> closestFieldOptional = objectUnderTest.getClosestField(propertyName, knownPropertyIds);
        assertThat(closestFieldOptional.isPresent(), is(true));
        assertThat(closestFieldOptional.get(), equalTo(testKnownPropertyId1));
    }
}