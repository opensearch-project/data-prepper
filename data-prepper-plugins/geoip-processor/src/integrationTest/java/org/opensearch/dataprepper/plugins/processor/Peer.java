/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

public class Peer {
    Ips IpsObject;
    private String host;

    // Getter Methods
    public Ips getIps() {
        return IpsObject;
    }

    public String getHost() {
        return host;
    }

    // Setter Methods
    public void setIps( Ips ipsObject ) {
        this.IpsObject = ipsObject;
    }

    public void setHost( String host ) {
        this.host = host;
    }
}