package org.opensearch.dataprepper.plugins.source.jira.configuration;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NameConfigTest {

    @Test
    void testValidProjectKeys_WithValidInput() {
        NameConfig nameConfig = new NameConfig();
        nameConfig.include.add("ABC123");
        nameConfig.include.add("XYZ789");
        nameConfig.exclude.add("TEST123");

        assertTrue(nameConfig.isValidProjectKeys());
    }

    @Test
    void testValidProjectKeys_WithEmptyLists() {
        NameConfig nameConfig = new NameConfig();
        assertTrue(nameConfig.isValidProjectKeys());
    }

    @Test
    void testValidProjectKeys_WithInvalidInclude() {
        NameConfig nameConfig = new NameConfig();
        nameConfig.include.add("ABC-123"); // Contains invalid character
        nameConfig.exclude.add("TEST123");

        assertFalse(nameConfig.isValidProjectKeys());
    }

    @Test
    void testValidProjectKeys_WithInvalidExclude() {
        NameConfig nameConfig = new NameConfig();
        nameConfig.include.add("ABC123");
        nameConfig.exclude.add("TEST@123"); // Contains invalid character

        assertFalse(nameConfig.isValidProjectKeys());
    }

    @Test
    void testCheckGivenListForRegex_WithNullValue() {
        NameConfig nameConfig = new NameConfig();
        List<String> testList = new ArrayList<>();
        testList.add(null);

        assertTrue(nameConfig.checkGivenListForRegex(testList));
    }

}
