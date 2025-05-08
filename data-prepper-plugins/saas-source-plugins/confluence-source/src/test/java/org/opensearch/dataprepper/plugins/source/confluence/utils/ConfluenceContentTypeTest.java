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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfluenceContentTypeTest {
    @Test
    void testEnumConstants() {
        assertNotNull(ConfluenceContentType.SPACE);
        assertNotNull(ConfluenceContentType.PAGE);
        assertNotNull(ConfluenceContentType.COMMENT);
        assertNotNull(ConfluenceContentType.ATTACHMENT);
        assertNotNull(ConfluenceContentType.BLOGPOST);
    }

    @Test
    void testTypeValues() {
        assertEquals("space", ConfluenceContentType.SPACE.getType());
        assertEquals("page", ConfluenceContentType.PAGE.getType());
        assertEquals("comment", ConfluenceContentType.COMMENT.getType());
        assertEquals("attachment", ConfluenceContentType.ATTACHMENT.getType());
        assertEquals("blogpost", ConfluenceContentType.BLOGPOST.getType());
    }
}
