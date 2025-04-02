/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;

import java.io.IOException;

/**
 * A S3ObjectHandler interface must be extended/implement for S3 Object parsing
 *
 */
public interface S3ObjectHandler {
    /**
     * Process S3 object content using S3 object reference and pushing to buffer
     * @param s3ObjectReference Contains bucket and s3 object details
     * @param acknowledgementSet acknowledgement set for the object
     * @param sourceCoordinator source coordinator
     * @param partitionKey partition key
     *
     * @throws IOException exception is thrown every time because this is not supported
     */
    void processS3Object(final S3ObjectReference s3ObjectReference,
                       final S3DataSelection dataSelection,
                       final AcknowledgementSet acknowledgementSet,
                       final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                       final String partitionKey) throws IOException;

    /**
     * delete S3 object using S3 object reference
     * @param s3ObjectReference Contains bucket and s3 object details
     */
    void deleteS3Object(final S3ObjectReference s3ObjectReference);

}
