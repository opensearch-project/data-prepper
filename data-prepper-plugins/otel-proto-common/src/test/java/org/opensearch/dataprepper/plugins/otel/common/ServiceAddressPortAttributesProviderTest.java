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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServiceAddressPortAttributesProviderTest {

    @Test
    void testGetAddressPortAttributeKeysList() {
        ServiceAddressPortAttributesProvider provider = new ServiceAddressPortAttributesProvider();
        List<ServiceAddressPortAttributesProvider.AddressPortAttributeKeys> keys = provider.getAddressPortAttributeKeysList();
        
        assertNotNull(keys);
        assertTrue(keys.size() > 0);
        assertEquals("server.address", keys.get(0).getAddress());
        assertEquals("server.port", keys.get(0).getPort());
    }

    @Test
    void testAddressPortAttributeKeys() {
        ServiceAddressPortAttributesProvider.AddressPortAttributeKeys keys = 
            new ServiceAddressPortAttributesProvider.AddressPortAttributeKeys("host.name,host.port");
        
        assertEquals("host.name", keys.getAddress());
        assertEquals("host.port", keys.getPort());
    }

    @Test
    void testAddressPortAttributeKeysInvalidFormat() {
        assertThrows(RuntimeException.class, () -> 
            new ServiceAddressPortAttributesProvider.AddressPortAttributeKeys("invalid"));
    }
}
