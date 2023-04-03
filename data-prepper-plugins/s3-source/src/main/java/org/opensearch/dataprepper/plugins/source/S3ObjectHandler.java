/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.io.IOException;

/**
 * A S3ObjectHandler interface must be extended/implement for S3 Object parsing
 *
 */
public interface S3ObjectHandler {
    void parseS3Object(final S3ObjectReference s3ObjectReference) throws IOException;
}
