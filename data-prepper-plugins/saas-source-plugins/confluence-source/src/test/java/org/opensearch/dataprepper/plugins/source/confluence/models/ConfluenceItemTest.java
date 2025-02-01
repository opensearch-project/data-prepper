/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.models;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class ConfluenceItemTest {


    private ConfluenceItem issueBean;

    @BeforeEach
    void setup() {
        issueBean = new ConfluenceItem();
    }

    @Test
    public void testInitialization() {
        assertNotNull(issueBean);
    }

    @Test
    public void testNull() {
        assertNull(issueBean.getId());
    }

    @Test
    void testNullCases() {
        assertEquals(issueBean.getUpdatedTimeMillis(), 0);
    }

    @Test
    void testGivenDateField() {
        Map<String, Object> fieldsTestObject = new HashMap<>();
        fieldsTestObject.put("created", "2024-07-06T21:12:23.437-0700");
        fieldsTestObject.put("updated", "2022-07-06T21:12:23.106-0700");
        assertEquals(issueBean.getCreatedTimeMillis(), 1720325543000L);
        assertEquals(issueBean.getUpdatedTimeMillis(), 1657167143000L);
    }

    @Test
    public void testStringSettersAndGetters() {
        String self = "selfTest";
        String key = "keyTest";
        String id = "idTest";
        String expand = "expandTest";

        issueBean.setId(id);
        assertEquals(issueBean.getId(), id);
    }

}
