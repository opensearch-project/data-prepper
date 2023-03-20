/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class S3Service {
    private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);

    private final S3ObjectHandler s3ObjectHandler;
    S3Service(final S3ObjectHandler s3ObjectHandler) {
        this.s3ObjectHandler =s3ObjectHandler;
    }

    void addS3Object(final S3ObjectReference s3ObjectReference) {
        try {
            s3ObjectHandler.parseS3Object(s3ObjectReference);
        } catch (final IOException e) {
            LOG.error("Unable to read S3 object from S3ObjectReference = {}", s3ObjectReference, e);
        }
    }
}
