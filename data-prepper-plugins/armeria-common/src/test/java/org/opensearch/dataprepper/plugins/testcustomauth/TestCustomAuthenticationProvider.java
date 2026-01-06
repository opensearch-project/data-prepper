/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.testcustomauth;

import com.linecorp.armeria.server.HttpService;
import io.grpc.ServerInterceptor;

import java.util.Optional;
import java.util.function.Function;

public interface TestCustomAuthenticationProvider {

    String UNAUTHENTICATED_PLUGIN_NAME = "unauthenticated";


    ServerInterceptor getAuthenticationInterceptor();

    default Optional<Function<? super HttpService, ? extends HttpService>> getHttpAuthenticationService() {
        return Optional.empty();
    }
}

