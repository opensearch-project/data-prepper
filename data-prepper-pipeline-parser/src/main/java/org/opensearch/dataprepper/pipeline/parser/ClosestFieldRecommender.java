/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import org.apache.commons.text.similarity.LevenshteinDistance;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

@Named
public class ClosestFieldRecommender {
    static final Integer MIN_DISTANCE_TO_RECOMMEND_PROPERTY = 3;

    private final LevenshteinDistance levenshteinDistance;

    @Inject
    public ClosestFieldRecommender(final LevenshteinDistance levenshteinDistance) {
        this.levenshteinDistance = levenshteinDistance;
    }

    public Optional<String> getClosestField(final String propertyName, final Collection<Object> knownPropertyIds) {
        String closestMatch = null;
        int smallestDistance = Integer.MAX_VALUE;

        for (final String field : knownPropertyIds.stream().map(Object::toString).collect(Collectors.toList())) {
            int distance = levenshteinDistance.apply(propertyName, field);

            if (distance < smallestDistance) {
                smallestDistance = distance;
                closestMatch = field;
            }
        }

        if (smallestDistance <= MIN_DISTANCE_TO_RECOMMEND_PROPERTY) {
            return Optional.ofNullable(closestMatch);
        }

        return Optional.empty();
    }
}
