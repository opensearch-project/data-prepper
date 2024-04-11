package org.opensearch.dataprepper.plugins.mongo.s3partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.util.ArrayList;
import java.util.List;

public class S3PartitionCreator {
    private static final Logger LOG = LoggerFactory.getLogger(S3PartitionCreator.class);
    final String bucketName;
    final String subFolder;
    final String region;
    final S3Client s3Client;

    S3PartitionCreator(final String bucketName, final String subFolder, final String region) {
        this.bucketName = bucketName;
        this.subFolder = subFolder;
        this.region = region;
        this.s3Client = S3Client.builder().region(Region.of(region)).build();
    }

    List<String> createPartition() {
        final List<String> partitions = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            String folderName = String.format("%02x", i) + "/";
            String key = subFolder + "/" + folderName;
            createPartition(key);
            partitions.add(folderName);
        }
        LOG.info("S3 partition created successfully.");
        return partitions;
    }

    private void createPartition(final String key) {
        try {
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(), RequestBody.empty());
        } catch (final Exception e) {
            LOG.error("Error creating partition {}", key, e);
            throw new RuntimeException(e);
        }
    }
}