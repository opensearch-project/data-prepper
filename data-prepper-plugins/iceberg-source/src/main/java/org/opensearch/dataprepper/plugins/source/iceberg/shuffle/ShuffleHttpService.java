/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.shuffle;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP service that serves shuffle data files to remote SHUFFLE_READ workers.
 */
public class ShuffleHttpService {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleHttpService.class);

    private final ShuffleStorage shuffleStorage;

    public ShuffleHttpService(final ShuffleStorage shuffleStorage) {
        this.shuffleStorage = shuffleStorage;
    }

    @Get("/{snapshotId}/{taskId}/index")
    public HttpResponse getIndex(@Param("snapshotId") final String snapshotId,
                                 @Param("taskId") final String taskId) {
        try {
            final ShuffleReader reader = shuffleStorage.createReader(snapshotId, taskId);
            final long[] offsets = reader.readIndex();
            reader.close();

            final byte[] bytes = new byte[offsets.length * Long.BYTES];
            java.nio.ByteBuffer.wrap(bytes).asLongBuffer().put(offsets);
            return HttpResponse.of(HttpStatus.OK, MediaType.OCTET_STREAM, bytes);
        } catch (final Exception e) {
            LOG.error("Failed to serve index for snapshot={} task={}", snapshotId, taskId, e);
            return HttpResponse.of(HttpStatus.NOT_FOUND);
        }
    }

    @Get("/{snapshotId}/{taskId}/data")
    public HttpResponse getData(@Param("snapshotId") final String snapshotId,
                                @Param("taskId") final String taskId,
                                @Param("offset") final long offset,
                                @Param("length") final int length) {
        if (length == 0) {
            return HttpResponse.of(HttpStatus.OK, MediaType.OCTET_STREAM, new byte[0]);
        }
        try {
            final ShuffleReader reader = shuffleStorage.createReader(snapshotId, taskId);
            final byte[] data = reader.readBytes(offset, length);
            reader.close();
            return HttpResponse.of(HttpStatus.OK, MediaType.OCTET_STREAM, data);
        } catch (final Exception e) {
            LOG.error("Failed to serve data for snapshot={} task={} offset={} length={}",
                    snapshotId, taskId, offset, length, e);
            return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
