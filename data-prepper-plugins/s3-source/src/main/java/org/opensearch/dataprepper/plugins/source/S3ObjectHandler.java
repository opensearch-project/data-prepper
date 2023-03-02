package org.opensearch.dataprepper.plugins.source;

import java.io.IOException;

import software.amazon.awssdk.core.SdkClient;

public interface S3ObjectHandler {
    void parseS3Object(final S3ObjectReference s3ObjectReference,final SdkClient sdkClient) throws IOException;
}
