/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class ServiceMapRelationshipTest {
    @Test
    void hash_is_consistent_with_static_known_values() {
        final String serviceName = "ServiceName";
        final String kind = "Kind";
        final String traceGroupName = "TraceGroupName";

        final ServiceMapRelationship objectUnderTest = ServiceMapRelationship.newDestinationRelationship(serviceName, kind, "d1", "r1", traceGroupName);

        assertThat(objectUnderTest.getHashId(), equalTo("r7rZwbptLFxOLPUlg/x2nA=="));
    }
}