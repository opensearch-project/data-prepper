/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;


public class InMemoryBufferFactory implements BufferFactory {
    @Override
    public Buffer getBuffer(LambdaAsyncClient lambdaAsyncClient, String functionName, String invocationType){
        return new InMemoryBuffer(lambdaAsyncClient, functionName, invocationType);
    }
}
