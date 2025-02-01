/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.BasicConfig;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.FilterConfig;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.NameConfig;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.Oauth2Config;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.PageTypeConfig;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.SpaceConfig;
import org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceConfigHelper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
public class ConfluenceConfigHelperTest {

    @Mock
    ConfluenceSourceConfig confluenceSourceConfig;

    @Mock
    FilterConfig filterConfig;

    @Mock
    PageTypeConfig pageTypeConfig;

    @Mock
    SpaceConfig spaceConfig;

    @Mock
    NameConfig nameConfig;

    @Mock
    AuthenticationConfig authenticationConfig;

    @Mock
    BasicConfig basicConfig;

    @Mock
    Oauth2Config oauth2Config;

    @Mock
    PluginConfigVariable accessTokenPluginConfigVariable;

    @Mock
    PluginConfigVariable refreshTokenPluginConfigVariable;

    @Test
    void testInitialization() {
        ConfluenceConfigHelper confluenceConfigHelper = new ConfluenceConfigHelper();
        assertNotNull(confluenceConfigHelper);
    }


    @Test
    void testGetIssueTypeFilter() {
        when(confluenceSourceConfig.getFilterConfig()).thenReturn(filterConfig);
        when(filterConfig.getPageTypeConfig()).thenReturn(pageTypeConfig);
        assertTrue(ConfluenceConfigHelper.getContentTypeIncludeFilter(confluenceSourceConfig).isEmpty());
        assertTrue(ConfluenceConfigHelper.getContentTypeExcludeFilter(confluenceSourceConfig).isEmpty());
        List<String> issueTypeFilter = List.of("Bug", "Story");
        List<String> issueTypeExcludeFilter = List.of("Bug2", "Story2");
        when(pageTypeConfig.getInclude()).thenReturn(issueTypeFilter);
        when(pageTypeConfig.getExclude()).thenReturn(issueTypeExcludeFilter);
        assertEquals(issueTypeFilter, ConfluenceConfigHelper.getContentTypeIncludeFilter(confluenceSourceConfig));
        assertEquals(issueTypeExcludeFilter, ConfluenceConfigHelper.getContentTypeExcludeFilter(confluenceSourceConfig));
    }

    @Test
    void testGetProjectNameFilter() {
        when(confluenceSourceConfig.getFilterConfig()).thenReturn(filterConfig);
        when(filterConfig.getSpaceConfig()).thenReturn(spaceConfig);
        when(spaceConfig.getNameConfig()).thenReturn(nameConfig);
        assertTrue(ConfluenceConfigHelper.getSpacesNameIncludeFilter(confluenceSourceConfig).isEmpty());
        assertTrue(ConfluenceConfigHelper.getSpacesNameExcludeFilter(confluenceSourceConfig).isEmpty());
        List<String> projectNameFilter = List.of("TEST", "TEST2");
        List<String> projectNameExcludeFilter = List.of("TEST3", "TEST4");
        when(nameConfig.getInclude()).thenReturn(projectNameFilter);
        when(nameConfig.getExclude()).thenReturn(projectNameExcludeFilter);
        assertEquals(projectNameFilter, ConfluenceConfigHelper.getSpacesNameIncludeFilter(confluenceSourceConfig));
        assertEquals(projectNameExcludeFilter, ConfluenceConfigHelper.getSpacesNameExcludeFilter(confluenceSourceConfig));
    }


    @Test
    void testValidateConfig() {
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));

        when(confluenceSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));

        when(confluenceSourceConfig.getAuthType()).thenReturn("fakeType");
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));
    }

    @Test
    void testValidateConfigBasic() {
        when(confluenceSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        when(confluenceSourceConfig.getAuthType()).thenReturn(BASIC);
        when(confluenceSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getBasicConfig()).thenReturn(basicConfig);
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));

        when(basicConfig.getUsername()).thenReturn("id");
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));

        when(basicConfig.getPassword()).thenReturn("credential");
        when(basicConfig.getUsername()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));

        when(basicConfig.getUsername()).thenReturn("id");
        assertDoesNotThrow(() -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));
    }

    @Test
    void testValidateConfigOauth2() {
        when(confluenceSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        when(confluenceSourceConfig.getAuthType()).thenReturn(OAUTH2);
        when(confluenceSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getOauth2Config()).thenReturn(oauth2Config);
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));

        when(oauth2Config.getAccessToken()).thenReturn(accessTokenPluginConfigVariable);
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));

        when(authenticationConfig.getOauth2Config().getRefreshToken()).thenReturn(refreshTokenPluginConfigVariable);
        when(oauth2Config.getAccessToken()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));

        when(oauth2Config.getAccessToken()).thenReturn(accessTokenPluginConfigVariable);
        assertDoesNotThrow(() -> ConfluenceConfigHelper.validateConfig(confluenceSourceConfig));
    }
}
