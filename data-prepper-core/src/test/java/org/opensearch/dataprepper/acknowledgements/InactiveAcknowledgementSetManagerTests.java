/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

public class InactiveAcknowledgementSetManagerTests {
    InactiveAcknowledgementSetManager acknowledgementSetManager;

    @BeforeEach
    void setup() {
        acknowledgementSetManager = InactiveAcknowledgementSetManager.getInstance();
    }

    @Test
    void testCreateAPI() {
        assertThat(acknowledgementSetManager, notNullValue());
        assertThrows(UnsupportedOperationException.class, () -> acknowledgementSetManager.create((a)->{}, Duration.ofMillis(10)));
    }

}
