/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class PipelineConditionalRouteTest {

    public static final String KNOWN_ROUTE_NAME = "testRouteName";
    public static final String KNOWN_CONDITION = "/my/property==value";

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));
    }

    @Test
    void serialize_single() throws IOException {
        final PipelineConditionalRoute conditionalRoute = new PipelineConditionalRoute(KNOWN_ROUTE_NAME, KNOWN_CONDITION);

        final String serialized = objectMapper.writeValueAsString(conditionalRoute);

        final InputStream inputStream = PluginModelTests.class.getResourceAsStream("conditional_route_single.yaml");
        assertThat(serialized, notNullValue());
        assertThat(serialized, equalTo(createStringFromInputStream(inputStream)));
    }

    @Test
    void deserialize_single() throws IOException {
        final InputStream inputStream = PluginModelTests.class.getResourceAsStream("conditional_route_single.yaml");
        final PipelineConditionalRoute conditionalRoute = objectMapper.readValue(inputStream, PipelineConditionalRoute.class);

        assertThat(conditionalRoute, notNullValue());
        assertThat(conditionalRoute.getName(), equalTo(KNOWN_ROUTE_NAME));
        assertThat(conditionalRoute.getCondition(), equalTo(KNOWN_CONDITION));
    }

    @Test
    void serialize_list() throws IOException {
        final List<PipelineConditionalRoute> routes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final String routeName = KNOWN_ROUTE_NAME + i;
            final String condition = KNOWN_CONDITION + i;
            final PipelineConditionalRoute route = new PipelineConditionalRoute(routeName, condition);
            routes.add(route);
        }

        final String serialized = objectMapper.writeValueAsString(routes);

        final InputStream inputStream = PluginModelTests.class.getResourceAsStream("conditional_route_list.yaml");
        assertThat(serialized, notNullValue());
        assertThat(serialized, equalTo(createStringFromInputStream(inputStream)));
    }

    @Test
    void deserialize_list() throws IOException {
        final InputStream inputStream = PluginModelTests.class.getResourceAsStream("conditional_route_list.yaml");

        final TypeReference<List<PipelineConditionalRoute>> listTypeReference = new TypeReference<>() {
        };
        final List<PipelineConditionalRoute> conditionalRouteList = objectMapper.readValue(inputStream, listTypeReference);

        assertThat(conditionalRouteList, notNullValue());
        assertThat(conditionalRouteList.size(), equalTo(3));

        for (int i = 0; i < 3; i++) {
            assertThat(conditionalRouteList.get(i), notNullValue());
            assertThat(conditionalRouteList.get(i).getName(), equalTo(KNOWN_ROUTE_NAME + i));
            assertThat(conditionalRouteList.get(i).getCondition(), equalTo(KNOWN_CONDITION + i));
        }
    }

    static String createStringFromInputStream(final InputStream inputStream) throws IOException {
        final StringBuilder stringBuilder = new StringBuilder();
        try (final Reader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName(StandardCharsets.UTF_8.name())))) {
            int counter = 0;
            while ((counter = reader.read()) != -1) {
                stringBuilder.append((char) counter);
            }
        }
        return stringBuilder.toString();
    }
}