/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import com.google.common.collect.Maps;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Collection;
import java.util.Map;

public class S3GroupManager {

    private final Map<S3GroupIdentifier, S3Group> allGroups = Maps.newConcurrentMap();
    private final S3SinkConfig s3SinkConfig;
    private final S3GroupIdentifierFactory s3GroupIdentifierFactory;
    private final BufferFactory bufferFactory;

    private final S3Client s3Client;

    public S3GroupManager(final S3SinkConfig s3SinkConfig,
                          final S3GroupIdentifierFactory s3GroupIdentifierFactory,
                          final BufferFactory bufferFactory,
                          final S3Client s3Client) {
        this.s3SinkConfig = s3SinkConfig;
        this.s3GroupIdentifierFactory = s3GroupIdentifierFactory;
        this.bufferFactory = bufferFactory;
        this.s3Client = s3Client;
    }

    public boolean hasNoGroups() {
        return allGroups.isEmpty();
    }

    public void removeGroup(final S3Group s3Group) {
        allGroups.remove(s3Group.getS3GroupIdentifier());
    }

    public Collection<S3Group> getS3GroupEntries() {
        return allGroups.values();
    }

    public S3Group getOrCreateGroupForEvent(final Event event) {

        final S3GroupIdentifier s3GroupIdentifier = s3GroupIdentifierFactory.getS3GroupIdentifierForEvent(event);

        if (allGroups.containsKey(s3GroupIdentifier)) {
            return allGroups.get(s3GroupIdentifier);
        } else {
            final Buffer bufferForNewGroup =  bufferFactory.getBuffer(s3Client, s3SinkConfig::getBucketName, s3GroupIdentifier::getGroupIdentifierFullObjectKey);
            final S3Group s3Group = new S3Group(s3GroupIdentifier, bufferForNewGroup);
            allGroups.put(s3GroupIdentifier, s3Group);
            return s3Group;
        }
    }


}
