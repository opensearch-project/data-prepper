/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

public class TestEventConfigurationContainer {
    public static EventConfiguration testEventConfiguration() {
        final EventConfiguration eventConfiguration = new EventConfiguration();
        eventConfiguration.setMaximumCachedKeys(0);
        return eventConfiguration;
    }
}
