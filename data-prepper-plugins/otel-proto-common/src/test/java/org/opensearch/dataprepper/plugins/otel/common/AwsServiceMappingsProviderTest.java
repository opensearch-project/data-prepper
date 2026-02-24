/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.common;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

class AwsServiceMappingsProviderTest {

    @Test
    void testGetServiceMappings() {
        AwsServiceMappingsProvider provider = new AwsServiceMappingsProvider();
        Map<String, String> mappings = provider.getServiceMappings();

        assertNotNull(mappings);
        assertTrue(mappings.size() > 0);
        assertThat(mappings.get("S3"), equalTo("AWS::S3"));
    }

}
