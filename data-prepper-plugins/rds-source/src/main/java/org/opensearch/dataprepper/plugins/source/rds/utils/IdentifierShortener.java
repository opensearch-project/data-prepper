/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class IdentifierShortener {

    public static String shortenIdentifier(final String identifier, final int maxLength) {
        if (identifier.length() <= maxLength) {
            return identifier;
        }

        try {
            // Create SHA-256 hash
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(identifier.getBytes(StandardCharsets.UTF_8));

            // Convert byte array to Base64 string
            String base64Hash = Base64.getUrlEncoder().withoutPadding().encodeToString(encodedhash);

            // Return the first maxLength characters
            return base64Hash.substring(0, Math.min(base64Hash.length(), maxLength));
        } catch (final NoSuchAlgorithmException e) {
            return identifier.substring(0, maxLength);
        }
    }
}
