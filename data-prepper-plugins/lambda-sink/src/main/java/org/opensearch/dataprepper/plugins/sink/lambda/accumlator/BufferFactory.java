/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.lambda.accumlator;

import software.amazon.awssdk.services.lambda.LambdaClient;

import java.io.IOException;

public interface BufferFactory {
    Buffer getBuffer(LambdaClient lambdaClient, String functionName, String invocationType) throws IOException;
}
