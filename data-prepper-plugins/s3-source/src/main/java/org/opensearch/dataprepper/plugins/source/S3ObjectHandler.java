/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.io.IOException;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

/**
 * A S3ObjectHandler interface must be extended/implement for S3 Object parsing
 *
 */
public interface S3ObjectHandler {
    /**
     * Parse S3 object content using S3 object reference and pushing to buffer
     * @param s3ObjectReference Contains bucket and s3 object details
     * @param acknowledgementSet acknowledgement set for the object
     *
     * @throws IOException exception is thrown every time because this is not supported
     */
    void parseS3Object(final S3ObjectReference s3ObjectReference, final AcknowledgementSet acknowledgementSet) throws IOException;
}
