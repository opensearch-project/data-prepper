/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

public class Peer {
    private String ip;
    private String host;

    // Getter Methods
    public String getIp() {
        return ip;
    }
    public String getHost() {
        return host;
    }
    // Setter Methods
    public void setIp( String ip ) {
        this.ip = ip;
    }
    public void setHost( String host ) {
        this.host = host;
    }
}