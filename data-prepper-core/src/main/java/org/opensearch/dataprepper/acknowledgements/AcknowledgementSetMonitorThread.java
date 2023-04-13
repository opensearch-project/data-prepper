/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import java.time.Duration;

class AcknowledgementSetMonitorThread {
    private final Thread monitorThread;
    private final AcknowledgementSetMonitor acknowledgementSetMonitor;
    private final Duration delayTime;
    private volatile boolean isStopped = false;

    public AcknowledgementSetMonitorThread(
            final AcknowledgementSetMonitor acknowledgementSetMonitor,
            final Duration delayTime) {
        this.acknowledgementSetMonitor = acknowledgementSetMonitor;
        this.delayTime = delayTime;
        monitorThread = new Thread(new Monitor());
        monitorThread.setDaemon(true);
        monitorThread.setName("acknowledgement-monitor");
    }

    public void start() {
        monitorThread.start();
    }

    public void stop() {
        isStopped = true;
    }

    private class Monitor implements Runnable {
        @Override
        public void run() {
            while (!isStopped) {
                acknowledgementSetMonitor.run();
                try {
                    Thread.sleep(delayTime.toMillis());
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
