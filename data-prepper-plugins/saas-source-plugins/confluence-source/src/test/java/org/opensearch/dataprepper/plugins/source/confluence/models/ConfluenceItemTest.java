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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class ConfluenceItemTest {


    private ConfluenceItem confluenceItemBean;

    @BeforeEach
    void setup() {
        confluenceItemBean = new ConfluenceItem();
    }

    @Test
    public void testInitialization() {
        assertNotNull(confluenceItemBean);
    }

    @Test
    public void testNull() {
        assertNull(confluenceItemBean.getId());
    }

    @Test
    void testNullCases() {
        assertEquals(confluenceItemBean.getUpdatedTimeMillis(), 0);
    }

    @Test
    void testGivenDateField() {
        assertEquals(confluenceItemBean.getCreatedTimeMillis(), 0L);
        assertEquals(confluenceItemBean.getUpdatedTimeMillis(), 0L);
    }

    @Test
    public void testStringSettersAndGetters() {
        String id = "idTest";

        confluenceItemBean.setId(id);
        assertEquals(confluenceItemBean.getId(), id);
    }

}
