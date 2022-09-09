/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;

/**
 * Execute entry into Data Prepper.
 */
@ComponentScan
public class DataPrepperExecute {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperExecute.class);

    public static void main(final String ... args) {
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        final ContextManager contextManager = new ContextManager();
        final DataPrepper dataPrepper = contextManager.getDataPrepperBean();

        LOG.trace("Starting Data Prepper execution");
        dataPrepper.execute();
    }
}
