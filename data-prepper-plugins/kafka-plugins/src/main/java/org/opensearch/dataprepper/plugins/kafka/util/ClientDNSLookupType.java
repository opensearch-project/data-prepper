/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum ClientDNSLookupType {

    DEFAULT("default"),
    USE_ALL_DNS_IPS("use_all_dns_ips"),
    CANONICAL_BOOTSTRAP("resolve_canonical_bootstrap_servers_only");
    private static final Map<String, ClientDNSLookupType> DNS_LOOKUP_TYPE_MAP = Arrays.stream(ClientDNSLookupType.values())
            .collect(Collectors.toMap(ClientDNSLookupType::toString, Function.identity()));

    private final String dnsLookupType;

    ClientDNSLookupType(String dnsLookupType) {
        this.dnsLookupType = dnsLookupType;
    }

    @Override
    public String toString() {
        return this.dnsLookupType;
    }

    public static ClientDNSLookupType getDnsLookupType(final String name) {
        return DNS_LOOKUP_TYPE_MAP.get(name.toLowerCase());
    }
}
