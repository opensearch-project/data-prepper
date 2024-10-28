/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

import java.io.IOException;

public interface BufferFactory {
    Buffer getBuffer(LambdaAsyncClient lambdaAsyncClient, String functionName, String invocationType) throws IOException;
}
