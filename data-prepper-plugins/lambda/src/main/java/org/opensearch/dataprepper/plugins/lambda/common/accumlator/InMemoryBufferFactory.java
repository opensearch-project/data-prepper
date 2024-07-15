/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import software.amazon.awssdk.services.lambda.LambdaClient;


public class InMemoryBufferFactory implements BufferFactory {
    @Override
    public Buffer getBuffer(LambdaClient lambdaClient, String functionName, String invocationType){
        return new InMemoryBuffer(lambdaClient, functionName, invocationType);
    }
}
