/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.util;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ConnectorTest {
    @Test
    void testGettersOfConnector() {
        final String name = "connectorName";
        final Boolean allowReplace = false;
        final Map<String, String> config = new HashMap<>();
        final Connector connector = new Connector(name, config, allowReplace);
        assertThat(connector.getName(), is(name));
        assertThat(connector.getConfig(), is(config));
        assertThat(connector.getConfig().get("name"), is(name));
        assertThat(connector.getAllowReplace(), is(allowReplace));
    }
}
