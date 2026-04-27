/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.http.client.Request;

import java.util.function.Function;

public class SignerProvider {
    private static final Function<Request, Request> NO_OP_SIGNER = r -> r;

    public static Function<Request, Request> getSigner() {
        final String authentication = System.getProperty("authentication");

        if(AwsRequestSigner.SIGNER_NAME.equals(authentication)) {
            return new AwsRequestSigner();
        }

        return NO_OP_SIGNER;
    }
}
