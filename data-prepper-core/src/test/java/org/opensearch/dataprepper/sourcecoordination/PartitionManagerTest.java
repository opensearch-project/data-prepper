/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public class PartitionManagerTest {

    private PartitionManager<String> objectUnderTest;

    @BeforeEach
    void setup() {
        objectUnderTest = new PartitionManager<>();
    }

    @Test
    void getActivePartition_returns_empty_optional_with_no_partition() {

        final Optional<SourcePartition<String>> result = objectUnderTest.getActivePartition();

        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void setActivePartition_followed_by_getActivePartition_returns_that_partition() {
        final SourcePartition<String> sourcePartition = mock(SourcePartition.class);

        objectUnderTest.setActivePartition(sourcePartition);

        final Optional<SourcePartition<String>> result = objectUnderTest.getActivePartition();

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(sourcePartition));
    }

    @Test
    void setActivePartition_followed_by_remove_and_get_returns_empty_optional() {
        final SourcePartition<String> sourcePartition = mock(SourcePartition.class);

        objectUnderTest.setActivePartition(sourcePartition);

        final Optional<SourcePartition<String>> result = objectUnderTest.getActivePartition();

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(sourcePartition));

        objectUnderTest.removeActivePartition();

        final Optional<SourcePartition<String>> emptyResult = objectUnderTest.getActivePartition();
        assertThat(emptyResult.isEmpty(), equalTo(true));
    }

}
