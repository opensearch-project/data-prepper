/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;

import java.io.IOException;

public class S3Service {
    private final S3ObjectHandler s3ObjectHandler;
    S3Service(final S3ObjectHandler s3ObjectHandler) {
        this.s3ObjectHandler = s3ObjectHandler;
    }

    void addS3Object(final S3ObjectReference s3ObjectReference, AcknowledgementSet acknowledgementSet) throws IOException {
        s3ObjectHandler.processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, acknowledgementSet, null, null);
    }

    void deleteS3Object(final S3ObjectReference s3ObjectReference) throws IOException {
        s3ObjectHandler.deleteS3Object(s3ObjectReference);
    }

}
