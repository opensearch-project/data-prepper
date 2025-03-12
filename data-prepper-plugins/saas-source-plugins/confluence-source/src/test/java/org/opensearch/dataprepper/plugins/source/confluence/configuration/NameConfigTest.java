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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NameConfigTest {

    @Test
    void testValidSpaceKeys_WithValidInput() {
        NameConfig nameConfig = new NameConfig();
        nameConfig.include.add("ABC123");
        nameConfig.include.add("XYZ789");
        nameConfig.exclude.add("TEST123");

        assertTrue(nameConfig.isValidSpaceKeys());
    }

    @Test
    void testValidSpaceKeys_WithEmptyLists() {
        NameConfig nameConfig = new NameConfig();
        assertTrue(nameConfig.isValidSpaceKeys());
    }

    @Test
    void testValidSpaceKeys_WithInvalidInclude() {
        NameConfig nameConfig = new NameConfig();
        nameConfig.include.add("ABC-123"); // Contains invalid character
        nameConfig.exclude.add("TEST123");

        assertFalse(nameConfig.isValidSpaceKeys());
    }

    @Test
    void testValidSpaceKeys_WithInvalidExclude() {
        NameConfig nameConfig = new NameConfig();
        nameConfig.include.add("ABC123");
        nameConfig.exclude.add("TEST@123"); // Contains invalid character

        assertFalse(nameConfig.isValidSpaceKeys());
    }

    @Test
    void testCheckGivenListForRegex_WithNullValue() {
        NameConfig nameConfig = new NameConfig();
        List<String> testList = new ArrayList<>();
        testList.add(null);

        assertTrue(nameConfig.checkGivenListForRegex(testList));
    }

}
