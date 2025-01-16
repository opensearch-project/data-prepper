/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.server;

import com.linecorp.armeria.server.Server;


public class CreateServer {
    private final ServerConfiguration serverConfiguration;

    //creating common class to start server pulling from start() of http source and otel sources

    //configure with what is needed for each source that is shared between all
    public CreateServer(final ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
    }

    //insert things specific to grpc or http
    public Server createGRPCServer() {

    }

    public Server createHTTPServer() {

    }
}
