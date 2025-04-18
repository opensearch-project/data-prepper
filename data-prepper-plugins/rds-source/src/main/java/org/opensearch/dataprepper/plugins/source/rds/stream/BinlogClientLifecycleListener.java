/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogClientLifecycleListener implements BinaryLogClient.LifecycleListener {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogClientLifecycleListener.class);

    @Override
    public void onConnect(BinaryLogClient binaryLogClient) {
        LOG.info("Binlog client connected.");
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {
        LOG.error("Binlog client communication failure.", e);
    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {
        LOG.error("Binlog client event deserialization failure.", e);
    }

    @Override
    public void onDisconnect(BinaryLogClient binaryLogClient) {
        LOG.info("Binlog client disconnected.");
    }
}
