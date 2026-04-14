/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SortConfig;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SortingOptionsTest {

    @Test
    void fromSortConfigs_with_null_returns_empty_list() {
        final List<SortingOptions> result = SortingOptions.fromSortConfigs(null);

        assertThat(result, notNullValue());
        assertThat(result, empty());
    }

    @Test
    void fromSortConfigs_with_empty_list_returns_empty_list() {
        final List<SortingOptions> result = SortingOptions.fromSortConfigs(Collections.emptyList());

        assertThat(result, notNullValue());
        assertThat(result, empty());
    }

    @Test
    void fromSortConfigs_with_ascending_order() {
        final SortConfig sortConfig = mock(SortConfig.class);
        when(sortConfig.getName()).thenReturn("@timestamp");
        when(sortConfig.getOrder()).thenReturn("ascending");

        final List<SortingOptions> result = SortingOptions.fromSortConfigs(List.of(sortConfig));

        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getFieldName(), equalTo("@timestamp"));
        assertThat(result.get(0).getOrder(), equalTo("asc"));
    }

    @Test
    void fromSortConfigs_with_descending_order() {
        final SortConfig sortConfig = mock(SortConfig.class);
        when(sortConfig.getName()).thenReturn("@timestamp");
        when(sortConfig.getOrder()).thenReturn("descending");

        final List<SortingOptions> result = SortingOptions.fromSortConfigs(List.of(sortConfig));

        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getFieldName(), equalTo("@timestamp"));
        assertThat(result.get(0).getOrder(), equalTo("desc"));
    }

    @Test
    void fromSortConfigs_with_multiple_configs() {
        final SortConfig timestampConfig = mock(SortConfig.class);
        when(timestampConfig.getName()).thenReturn("@timestamp");
        when(timestampConfig.getOrder()).thenReturn("descending");

        final SortConfig idConfig = mock(SortConfig.class);
        when(idConfig.getName()).thenReturn("_id");
        when(idConfig.getOrder()).thenReturn("ascending");

        final List<SortingOptions> result = SortingOptions.fromSortConfigs(List.of(timestampConfig, idConfig));

        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0).getFieldName(), equalTo("@timestamp"));
        assertThat(result.get(0).getOrder(), equalTo("desc"));
        assertThat(result.get(1).getFieldName(), equalTo("_id"));
        assertThat(result.get(1).getOrder(), equalTo("asc"));
    }
}
