package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.InetAddressValidator;

class DiscoveryUtils {
    static boolean validateEndpoint(final String endpoint) {
        return DomainValidator.getInstance(true).isValid(endpoint) || InetAddressValidator.getInstance().isValid(endpoint);
    }
}
