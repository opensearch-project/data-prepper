/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

