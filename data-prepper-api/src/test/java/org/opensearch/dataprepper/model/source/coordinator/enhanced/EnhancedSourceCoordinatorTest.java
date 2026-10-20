/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class EnhancedSourceCoordinatorTest {
    @Test
    void testRenewPartitionOwnershipDoesNothingByDefault() {
        final EnhancedSourceCoordinator objectUnderTest = mock(EnhancedSourceCoordinator.class);
        final EnhancedSourcePartition<String> partition = mock(EnhancedSourcePartition.class);
        doCallRealMethod().when(objectUnderTest).renewPartitionOwnership(partition);

        objectUnderTest.renewPartitionOwnership(partition);

        verifyNoMoreInteractions(partition);
    }
}
