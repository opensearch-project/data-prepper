/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AcknowledgementSetMonitorThreadTest {
    @Mock
    private AcknowledgementSetMonitor acknowledgementSetMonitor;
    private Duration delayTime;

    @BeforeEach
    void setUp() {
        delayTime = Duration.ofMillis(10);
    }

    private AcknowledgementSetMonitorThread createObjectUnderTest() {
        return new AcknowledgementSetMonitorThread(acknowledgementSetMonitor, delayTime);
    }

    @Test
    void run_will_call_monitor_run() {
        final AcknowledgementSetMonitorThread objectUnderTest = createObjectUnderTest();

        objectUnderTest.start();
        await().atMost(delayTime.plusMillis(500))
                .untilAsserted(() -> {
                    verify(acknowledgementSetMonitor, atLeastOnce()).run();
                });

        verify(acknowledgementSetMonitor, atLeastOnce()).run();

        objectUnderTest.stop();
    }
}