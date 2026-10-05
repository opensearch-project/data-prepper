/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class HecTokenValidator {

    private final List<byte[]> validTokens;
    private final List<byte[]> disabledTokens;
    private final Map<String, HecTokenConfig.HecTokenDefaults> tokenDefaults;

    public HecTokenValidator(final List<HecTokenConfig> tokenConfigs) {
        Objects.requireNonNull(tokenConfigs, "tokenConfigs must not be null");
        final List<byte[]> tokens = new ArrayList<>();
        final List<byte[]> disabled = new ArrayList<>();
        final Map<String, HecTokenConfig.HecTokenDefaults> defaults = new HashMap<>();
        for (final HecTokenConfig config : tokenConfigs) {
            final String token = config.getToken();
            if (token == null || token.isBlank()) {
                throw new IllegalArgumentException("HEC token value must not be null or blank");
            }
            final byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
            if (config.isEnabled()) {
                tokens.add(tokenBytes);
            } else {
                disabled.add(tokenBytes);
            }
            if (config.getDefaults() != null) {
                defaults.put(token, config.getDefaults());
            }
        }
        this.validTokens = Collections.unmodifiableList(tokens);
        this.disabledTokens = Collections.unmodifiableList(disabled);
        this.tokenDefaults = Collections.unmodifiableMap(defaults);
    }

    public boolean isValid(final String token) {
        return matchesAny(token, validTokens);
    }

    public boolean isDisabled(final String token) {
        return matchesAny(token, disabledTokens);
    }

    private static boolean matchesAny(final String token, final List<byte[]> candidates) {
        if (token == null) {
            return false;
        }
        final byte[] presented = token.getBytes(StandardCharsets.UTF_8);
        boolean match = false;
        for (final byte[] candidate : candidates) {
            if (MessageDigest.isEqual(candidate, presented)) {
                match = true;
            }
        }
        return match;
    }

    public Optional<HecTokenConfig.HecTokenDefaults> getDefaults(final String token) {
        return Optional.ofNullable(tokenDefaults.get(token));
    }

    public Optional<String> extractToken(final String authorizationHeader) {
        if (authorizationHeader == null || authorizationHeader.isBlank()) {
            return Optional.empty();
        }
        final String prefix = "Splunk ";
        if (!authorizationHeader.startsWith(prefix)) {
            return Optional.empty();
        }
        final String token = authorizationHeader.substring(prefix.length()).trim();
        if (token.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(token);
    }
}
