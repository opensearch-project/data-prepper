/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

class AcknowledgementSetMonitorThread {
    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgementSetMonitorThread.class);
    private final Thread monitorThread;
    private final AcknowledgementSetMonitor acknowledgementSetMonitor;
    private final Duration delayTime;
    private final Duration shutdownTimeout;
    private volatile boolean isStopped = false;

    public AcknowledgementSetMonitorThread(
            final AcknowledgementSetMonitor acknowledgementSetMonitor,
            final Duration delayTime,
            final Duration shutdownTimeout) {
        this.acknowledgementSetMonitor = acknowledgementSetMonitor;
        this.delayTime = delayTime;
        this.shutdownTimeout = shutdownTimeout;
        monitorThread = new Thread(new Monitor());
        monitorThread.setDaemon(true);
        monitorThread.setName("acknowledgement-monitor");
    }

    public void start() {
        monitorThread.start();
    }

    public void stop() {
        isStopped = true;
        monitorThread.interrupt();
        try {
            monitorThread.join(shutdownTimeout.toMillis());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (monitorThread.isAlive()) {
            LOG.warn("The acknowledgement-monitor thread did not stop within {}.", shutdownTimeout);
        }
    }

    private class Monitor implements Runnable {
        @Override
        public void run() {
            while (!isStopped) {
                acknowledgementSetMonitor.run();
                try {
                    Thread.sleep(delayTime.toMillis());
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}
