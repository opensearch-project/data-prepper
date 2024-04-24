/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import com.google.common.collect.Maps;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.s3.codec.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class S3GroupManager {

    private static final Logger LOG = LoggerFactory.getLogger(S3GroupManager.class);
    private final Map<S3GroupIdentifier, S3Group> allGroups = Maps.newConcurrentMap();
    private final S3SinkConfig s3SinkConfig;
    private final S3GroupIdentifierFactory s3GroupIdentifierFactory;
    private final BufferFactory bufferFactory;

    private final CodecFactory codecFactory;

    private final S3AsyncClient s3Client;

    private long totalGroupSize;


    public S3GroupManager(final S3SinkConfig s3SinkConfig,
                          final S3GroupIdentifierFactory s3GroupIdentifierFactory,
                          final BufferFactory bufferFactory,
                          final CodecFactory codecFactory,
                          final S3AsyncClient s3Client) {
        this.s3SinkConfig = s3SinkConfig;
        this.s3GroupIdentifierFactory = s3GroupIdentifierFactory;
        this.bufferFactory = bufferFactory;
        this.codecFactory = codecFactory;
        this.s3Client = s3Client;
        totalGroupSize = 0;
    }

    public boolean hasNoGroups() {
        return allGroups.isEmpty();
    }

    public int getNumberOfGroups() { return allGroups.size(); }

    public void removeGroup(final S3Group s3Group) {
        allGroups.remove(s3Group.getS3GroupIdentifier());
    }

    public Collection<S3Group> getS3GroupEntries() {
        return allGroups.values();
    }

    public Collection<S3Group> getS3GroupsSortedBySize() {
        return allGroups.values().stream().sorted(Collections.reverseOrder()).collect(Collectors.toList());
    }

    public S3Group getOrCreateGroupForEvent(final Event event) {

        final S3GroupIdentifier s3GroupIdentifier = s3GroupIdentifierFactory.getS3GroupIdentifierForEvent(event);

        if (allGroups.containsKey(s3GroupIdentifier)) {
            return allGroups.get(s3GroupIdentifier);
        } else {
            final Buffer bufferForNewGroup =  bufferFactory.getBuffer(s3Client, s3GroupIdentifier::getFullBucketName, s3GroupIdentifier::getGroupIdentifierFullObjectKey, s3SinkConfig.getDefaultBucket());
            final OutputCodec outputCodec = codecFactory.provideCodec();
            final S3Group s3Group = new S3Group(s3GroupIdentifier, bufferForNewGroup, outputCodec);
            allGroups.put(s3GroupIdentifier, s3Group);
            LOG.debug("Created a new S3 group. Total number of groups: {}", allGroups.size());
            return s3Group;
        }
    }

    public long recalculateAndGetGroupSize() {
        long totalSize = 0;

        for (final S3Group s3Group : allGroups.values()) {
            totalSize += s3Group.getBuffer().getSize();
        }

        return totalSize;
    }
}
