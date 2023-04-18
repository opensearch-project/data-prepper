/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

public class S3SelectResponseHandlerFactory {

    public S3SelectResponseHandler provideS3SelectResponseHandler() {
        return new S3SelectResponseHandler();
    }
}
