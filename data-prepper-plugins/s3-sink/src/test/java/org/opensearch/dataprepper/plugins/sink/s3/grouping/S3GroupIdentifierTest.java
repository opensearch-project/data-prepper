/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;


import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class S3GroupIdentifierTest {

    @Test
    void S3GroupIdentifier_with_the_same_identificationHash_and_different_fullObjectKey_are_considered_equal() {
        final Map<String, Object> identificationHash = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String groupOneFullObjectKey = UUID.randomUUID().toString();
        final String groupTwoFullObjectKey = UUID.randomUUID().toString();

        final S3GroupIdentifier s3GroupIdentifier = new S3GroupIdentifier(identificationHash, groupOneFullObjectKey);
        final S3GroupIdentifier seconds3GroupIdentifier = new S3GroupIdentifier(identificationHash, groupTwoFullObjectKey);

        assertThat(s3GroupIdentifier.equals(seconds3GroupIdentifier), equalTo(true));
        assertThat(s3GroupIdentifier.hashCode(), equalTo(seconds3GroupIdentifier.hashCode()));
    }

    @Test
    void S3GroupIdentifier_with_different_identificationHash_is_not_considered_equal() {
        final Map<String, Object> identificationHashOne = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final Map<String, Object> identificationHashTwo = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String groupOneFullObjectKey = UUID.randomUUID().toString();
        final String groupTwoFullObjectKey = UUID.randomUUID().toString();

        final S3GroupIdentifier s3GroupIdentifier = new S3GroupIdentifier(identificationHashOne, groupOneFullObjectKey);
        final S3GroupIdentifier seconds3GroupIdentifier = new S3GroupIdentifier(identificationHashTwo, groupTwoFullObjectKey);

        assertThat(s3GroupIdentifier.equals(seconds3GroupIdentifier), equalTo(false));
        assertNotEquals(s3GroupIdentifier.hashCode(), seconds3GroupIdentifier.hashCode());
    }
}
