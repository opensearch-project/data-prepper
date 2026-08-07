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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ServiceAddressPortAttributesProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceAddressPortAttributesProvider.class);
    private static final String HOST_PORT_ORDERED_LIST_FILE = "hostport_attributes_ordered_list";
    List<AddressPortAttributeKeys> addressPortAttributeKeys;
    public ServiceAddressPortAttributesProvider() {
        try (
            final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(HOST_PORT_ORDERED_LIST_FILE);
        ) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                addressPortAttributeKeys =  reader.lines()
                .filter(line -> line.contains(","))
                .map(this::getAddressPortAttributeKeys)
                .collect(Collectors.toList());
            } catch (IOException e) {
                throw e;
            }
        } catch (final Exception e) {
            addressPortAttributeKeys = null;
            LOG.error("An exception occurred while initializing hostport attribute list for Data Prepper", e);
        }
    }
    public List<AddressPortAttributeKeys> getAddressPortAttributeKeysList() {
        return addressPortAttributeKeys;
    }

    private AddressPortAttributeKeys getAddressPortAttributeKeys(final String str) {
        return new AddressPortAttributeKeys(str);
    }

    public static class AddressPortAttributeKeys {
        final String address;
        final String port;
        public AddressPortAttributeKeys(final String str) {
            String[] fields = str.split(",");
            if (fields.length != 2) {
                throw new RuntimeException("Invalid address-port string");
            }
            this.address = fields[0];
            this.port = fields[1];
        }

        public String getAddress() {
            return address;
        }

        public String getPort() {
            return port;
        }
    }
}
