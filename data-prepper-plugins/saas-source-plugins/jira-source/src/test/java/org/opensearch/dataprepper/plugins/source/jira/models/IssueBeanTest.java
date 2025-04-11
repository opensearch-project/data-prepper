/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.models;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT;

@ExtendWith(MockitoExtension.class)
public class IssueBeanTest {

    @Mock
    private Map<String, Object> renderedFieldsTestObject;

    @Mock
    private Map<String, Object> propertiesTestObject;

    @Mock
    private Map<String, String> namesTestObject;

    @Mock
    private Map<String, Object> fieldsTestObject;

    private IssueBean issueBean;

    @BeforeEach
    void setup() {
        issueBean = new IssueBean();
    }

    @Test
    public void testInitialization() {
        assertNotNull(issueBean);
    }

    @Test
    public void testNull() {
        assertNull(issueBean.getExpand());
        assertNull(issueBean.getId());
        assertNull(issueBean.getSelf());
        assertNull(issueBean.getKey());
        assertNull(issueBean.getRenderedFields());
        assertNull(issueBean.getProperties());
        assertNull(issueBean.getNames());
        assertNull(issueBean.getFields());
    }

    @Test
    void testNullCases() {
        assertNull(issueBean.getProject());
        assertNull(issueBean.getProjectName());
        assertEquals(issueBean.getUpdatedTimeMillis(), 0);
    }

    @Test
    void testGivenDateField() {
        Map<String, Object> fieldsTestObject = new HashMap<>();
        fieldsTestObject.put("created", "2024-07-06T21:12:23.437-0700");
        fieldsTestObject.put("updated", "2022-07-06T21:12:23.106-0700");
        issueBean.setFields(fieldsTestObject);
        assertEquals(1720325543437L, issueBean.getCreatedTimeMillis());
        assertEquals(1657167143106L, issueBean.getUpdatedTimeMillis());
    }

    @Test
    public void testStringSettersAndGetters() {
        String self = "selfTest";
        String key = "keyTest";
        String id = "idTest";
        String expand = "expandTest";

        issueBean.setExpand(expand);
        assertEquals(issueBean.getExpand(), expand);
        issueBean.setId(id);
        assertEquals(issueBean.getId(), id);
        issueBean.setSelf(self);
        assertEquals(issueBean.getSelf(), self);
        issueBean.setKey(key);
        assertEquals(issueBean.getKey(), key);
    }

    @Test
    public void testMapSettersAndGetters() {

        issueBean.setRenderedFields(renderedFieldsTestObject);
        assertEquals(issueBean.getRenderedFields(), renderedFieldsTestObject);
        issueBean.setProperties(propertiesTestObject);
        assertEquals(issueBean.getProperties(), propertiesTestObject);
        issueBean.setNames(namesTestObject);
        assertEquals(issueBean.getNames(), namesTestObject);
        issueBean.setFields(fieldsTestObject);
        assertEquals(issueBean.getFields(), fieldsTestObject);
    }

    @Test
    public void testFieldPropertyGetters() {
        Map<String, Object> fieldsTestObject = new HashMap<>();
        Map<String, Object> projectTestObject = new HashMap<>();
        String projectName = "name of project";
        String projectKey = "PROJKEY";
        projectTestObject.put(KEY, projectKey);
        projectTestObject.put(NAME, projectName);
        fieldsTestObject.put(PROJECT, projectTestObject);

        issueBean.setFields(fieldsTestObject);
        assertEquals(projectKey, issueBean.getProject());
        assertEquals(projectName, issueBean.getProjectName());
    }

}
