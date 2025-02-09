/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum ConfluenceContentType {
    SPACE("SPACE"),
    PAGE("PAGE"),
    BLOGPOST("BLOGPOST"),
    COMMENT("COMMENT"),
    ATTACHMENT("ATTACHMENT");

    @Getter
    private final String type;

    public static ConfluenceContentType fromString(String value) {
        for (ConfluenceContentType contentType : ConfluenceContentType.values()) {
            if (contentType.type.equalsIgnoreCase(value)) {
                return contentType;
            }
        }
        return null;
    }
}
