/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import com.google.common.net.InternetDomainName;
import org.apache.commons.validator.routines.InetAddressValidator;

class DiscoveryUtils {
    static boolean validateEndpoint(final String endpoint) {
        return InternetDomainName.isValid(endpoint) || InetAddressValidator.getInstance().isValid(endpoint);
    }
}
