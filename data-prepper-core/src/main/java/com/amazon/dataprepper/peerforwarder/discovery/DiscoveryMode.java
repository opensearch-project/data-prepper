/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder.discovery;

public enum DiscoveryMode {
    // TODO: Add peer list provider function for each enum type
    STATIC("static"),
    DNS("dns"),
    AWS_CLOUD_MAP("aws_cloud_map");

    private final String mode;

    DiscoveryMode(String mode) {
        this.mode = mode;
    }
}
