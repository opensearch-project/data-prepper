/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JiraContentTypeTest {
    @Test
    void testEnumConstants() {
        assertNotNull(JiraContentType.PROJECT);
        assertNotNull(JiraContentType.ISSUE);
        assertNotNull(JiraContentType.COMMENT);
        assertNotNull(JiraContentType.ATTACHMENT);
        assertNotNull(JiraContentType.WORKLOG);
    }

    @Test
    void testTypeValues() {
        assertEquals("PROJECT", JiraContentType.PROJECT.getType());
        assertEquals("ISSUE", JiraContentType.ISSUE.getType());
        assertEquals("COMMENT", JiraContentType.COMMENT.getType());
        assertEquals("ATTACHMENT", JiraContentType.ATTACHMENT.getType());
        assertEquals("WORKLOG", JiraContentType.WORKLOG.getType());
    }
}
