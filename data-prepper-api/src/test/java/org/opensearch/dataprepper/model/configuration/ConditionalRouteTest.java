/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionalRouteTest {

    public static final String KNOWN_ROUTE_NAME = "testRouteName";
    public static final String KNOWN_CONDITION = "/my/property==value";

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));
    }

    @Test
    void serialize_single() throws IOException {
        final ConditionalRoute conditionalRoute = new ConditionalRoute(KNOWN_ROUTE_NAME, KNOWN_CONDITION);

        final String serialized = objectMapper.writeValueAsString(conditionalRoute);

        final InputStream inputStream = PluginModelTests.class.getResourceAsStream("conditional_route_single.yaml");
        assertThat(serialized, notNullValue());
        assertThat(serialized, equalTo(createStringFromInputStream(inputStream)));
    }

    @Test
    void deserialize_single() throws IOException {
        final InputStream inputStream = PluginModelTests.class.getResourceAsStream("conditional_route_single.yaml");
        final ConditionalRoute conditionalRoute = objectMapper.readValue(inputStream, ConditionalRoute.class);

        assertThat(conditionalRoute, notNullValue());
        assertThat(conditionalRoute.getName(), equalTo(KNOWN_ROUTE_NAME));
        assertThat(conditionalRoute.getCondition(), equalTo(KNOWN_CONDITION));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "conditional_route_invalid_object.yaml",
            "conditional_route_invalid_non_string.yaml",
            "conditional_route_invalid_just_value.yaml"
    })
    void deserialize_single_invalid(final String invalidResourceName) throws IOException {
        final InputStream inputStream = PluginModelTests.class.getResourceAsStream(invalidResourceName);

        final String invalidYaml = createStringFromInputStream(inputStream);

        final InvalidFormatException actualException = assertThrows(InvalidFormatException.class, () ->
                objectMapper.readValue(invalidYaml, ConditionalRoute.class));

        assertThat(actualException.getMessage(), containsString("Route"));
    }

    @Test
    void serialize_list() throws IOException {
        final List<ConditionalRoute> routes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final String routeName = KNOWN_ROUTE_NAME + i;
            final String condition = KNOWN_CONDITION + i;
            final ConditionalRoute route = new ConditionalRoute(routeName, condition);
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

        final TypeReference<List<ConditionalRoute>> listTypeReference = new TypeReference<>() {
        };
        final List<ConditionalRoute> conditionalRouteList = objectMapper.readValue(inputStream, listTypeReference);

        assertThat(conditionalRouteList, notNullValue());
        assertThat(conditionalRouteList.size(), equalTo(3));

        for (int i = 0; i < 3; i++) {
            assertThat(conditionalRouteList.get(i), notNullValue());
            assertThat(conditionalRouteList.get(i).getName(), equalTo(KNOWN_ROUTE_NAME + i));
            assertThat(conditionalRouteList.get(i).getCondition(), equalTo(KNOWN_CONDITION + i));
        }
    }

    static String createStringFromInputStream(final InputStream inputStream) throws IOException {
        return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
}