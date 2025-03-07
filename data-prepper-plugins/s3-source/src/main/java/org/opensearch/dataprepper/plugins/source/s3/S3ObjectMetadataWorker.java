/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Class responsible for taking an {@link S3ObjectReference} and creating all the necessary {@link Event}
 * objects in the Data Prepper {@link Buffer}.
 */
class S3ObjectMetadataWorker extends S3ObjectWorker {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectWorker.class);
    private static final long DEFAULT_CHECKPOINT_INTERVAL_MILLS = 5 * 60_000;

    private static final int MAX_RETRIES_DELETE_OBJECT = 3;
    private static final long DELETE_OBJECT_RETRY_DELAY_MS = 1000;

    public S3ObjectMetadataWorker(final S3ObjectRequest s3ObjectRequest) {
        super(s3ObjectRequest);
    }

    @Override
    public long consumeS3Object(final S3ObjectReference s3ObjectReference, final S3InputFile inputFile, Consumer<Record<Event>> consumer) throws Exception {
        final String BUCKET = "bucket";
        final String KEY = "key";
        final String TIME = "time";
        final String LENGTH = "length";
        Map<String, Object> data = new HashMap<>();
        data.put(BUCKET, s3ObjectReference.getBucketName());
        data.put(KEY, s3ObjectReference.getKey());
        data.put(TIME, inputFile.getLastModified());
        data.put(LENGTH, inputFile.getLength());
        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(data)
                .build();
        consumer.accept(new Record<>(event));
        return event.toJsonString().length();
    }

    @Override
    public void deleteS3Object(final S3ObjectReference s3ObjectReference) {
        throw new RuntimeException("Not supported");
    }
    
}
