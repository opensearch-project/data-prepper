/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


class S3PartitionCreatorTest {

    @Test
    void test_createPartition_create_correct_number_of_distinct_partition_strings() {
        final int partitionCount = new Random().nextInt(10) + 1;
        final S3PartitionCreator s3PartitionCreator = createObjectUnderTest(partitionCount);

        final List<String> partitions = s3PartitionCreator.createPartitions();

        assertThat(partitions.size(), is(partitionCount));
        assertThat(new HashSet<>(partitions).size(), is(partitionCount));
    }

    private S3PartitionCreator createObjectUnderTest(final int partitionCount) {
        return new S3PartitionCreator(partitionCount);
    }
}