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
        assertNotNull(ConfluenceContentType.PROJECT);
        assertNotNull(ConfluenceContentType.ISSUE);
        assertNotNull(ConfluenceContentType.COMMENT);
        assertNotNull(ConfluenceContentType.ATTACHMENT);
        assertNotNull(ConfluenceContentType.WORKLOG);
    }

    @Test
    void testTypeValues() {
        assertEquals("PROJECT", ConfluenceContentType.PROJECT.getType());
        assertEquals("ISSUE", ConfluenceContentType.ISSUE.getType());
        assertEquals("COMMENT", ConfluenceContentType.COMMENT.getType());
        assertEquals("ATTACHMENT", ConfluenceContentType.ATTACHMENT.getType());
        assertEquals("WORKLOG", ConfluenceContentType.WORKLOG.getType());
    }
}
