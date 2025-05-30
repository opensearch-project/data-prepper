/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.utils;

import java.net.InetAddress;
import java.util.Random;

public class ServerIdGenerator {
    static final int MIN_SERVER_ID = 100_000;
    static final int MAX_SERVER_ID = 999_999;

    public static int generateServerId() {
        try {
            // Get local IP address
            String hostAddress = InetAddress.getLocalHost().getHostAddress();

            // Get process-specific info
            long processId = ProcessHandle.current().pid();
            long currentTime = System.currentTimeMillis() % MIN_SERVER_ID;

            int hash = Math.abs((hostAddress + processId + currentTime).hashCode());

            return MIN_SERVER_ID + (hash % (MAX_SERVER_ID - MIN_SERVER_ID + 1));

        } catch (Exception e) {
            // Fallback to random number
            return MIN_SERVER_ID + new Random().nextInt(MAX_SERVER_ID - MIN_SERVER_ID + 1);
        }
    }
}
