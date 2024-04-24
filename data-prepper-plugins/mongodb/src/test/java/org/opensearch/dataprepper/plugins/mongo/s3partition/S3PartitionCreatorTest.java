package org.opensearch.dataprepper.plugins.mongo.s3partition;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class S3PartitionCreatorTest {

    @Test
    public void createPartitionTest() {
        final int partitionCount = Math.abs(new Random().nextInt(1000));
        final S3PartitionCreator s3PartitionCreator = new S3PartitionCreator(partitionCount);
        final List<String> partitions = s3PartitionCreator.createPartition();
        assertThat(partitions, hasSize(partitionCount));
    }
}
