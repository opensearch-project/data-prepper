/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.confluence.configuration;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageTypeConfigTest {

    @Test
    void testValidPageType_WithValidInput() {
        PageTypeConfig pageTypeConfig = new PageTypeConfig();
        pageTypeConfig.include.add("page");
        pageTypeConfig.include.add("comment");
        pageTypeConfig.exclude.add("blogpost");

        assertTrue(pageTypeConfig.isValidPageType());
    }

    @Test
    void testValidPageType_WithOverlappingInput() {
        PageTypeConfig pageTypeConfig = new PageTypeConfig();
        pageTypeConfig.include.add("page");
        pageTypeConfig.include.add("comment");
        pageTypeConfig.exclude.add("page");
        pageTypeConfig.exclude.add("blogpost");

        assertFalse(pageTypeConfig.isValidPageType());
    }

    @Test
    void testValidPageType_WithNoOverlappingInput_but_null_include() {
        PageTypeConfig pageTypeConfig = new PageTypeConfig();
        pageTypeConfig.exclude.add("page");
        pageTypeConfig.exclude.add("blogpost");
        assertTrue(pageTypeConfig.isValidPageType());
    }

    @Test
    void testValidPageType_WithNoOverlappingInput_but_null_exclude() {
        PageTypeConfig pageTypeConfig = new PageTypeConfig();
        pageTypeConfig.include.add("page");
        pageTypeConfig.include.add("blogpost");
        assertTrue(pageTypeConfig.isValidPageType());
    }

    @Test
    void testValidSpaceKeys_WithEmptyLists() {
        PageTypeConfig pageTypeConfig = new PageTypeConfig();
        assertTrue(pageTypeConfig.isValidPageType());
    }

    @Test
    void testValidSpaceKeys_WithInvalidInclude() {
        PageTypeConfig pageTypeConfig = new PageTypeConfig();
        pageTypeConfig.include.add("BlogPost"); // Contains invalid character
        pageTypeConfig.exclude.add("Page");
        pageTypeConfig.exclude.add("page");
        pageTypeConfig.include.add("<<page>>");

        assertFalse(pageTypeConfig.isValidPageType());
    }

    @Test
    void testValidSpaceKeys_WithInvalidExclude() {
        PageTypeConfig pageTypeConfig = new PageTypeConfig();
        pageTypeConfig.include.add("page");
        pageTypeConfig.exclude.add("BlogPost");
        pageTypeConfig.exclude.add("Test@123");// Contains invalid character

        assertFalse(pageTypeConfig.isValidPageType());
    }

    @Test
    void testCheckGivenListForRegex_WithNullValue() {
        PageTypeConfig pageTypeConfig = new PageTypeConfig();
        pageTypeConfig.include.add(null);
        assertFalse(pageTypeConfig.isValidPageType());
    }

}
