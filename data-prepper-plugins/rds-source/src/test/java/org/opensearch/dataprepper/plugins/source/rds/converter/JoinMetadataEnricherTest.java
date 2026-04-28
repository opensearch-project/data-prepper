/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.plugins.source.rds.configuration.JoinRelation;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.opensearch.dataprepper.plugins.source.rds.converter.JoinMetadataEnricher.JOIN_CHILD_PK_NAME_METADATA;
import static org.opensearch.dataprepper.plugins.source.rds.converter.JoinMetadataEnricher.JOIN_CHILD_PK_VALUE_METADATA;
import static org.opensearch.dataprepper.plugins.source.rds.converter.JoinMetadataEnricher.JOIN_FIELDS_METADATA;
import static org.opensearch.dataprepper.plugins.source.rds.converter.JoinMetadataEnricher.JOIN_IS_DELETE_METADATA;
import static org.opensearch.dataprepper.plugins.source.rds.converter.JoinMetadataEnricher.JOIN_IS_PARENT_METADATA;
import static org.opensearch.dataprepper.plugins.source.rds.converter.JoinMetadataEnricher.JOIN_PRIMARY_KEY_METADATA;
import static org.opensearch.dataprepper.plugins.source.rds.converter.JoinMetadataEnricher.JOIN_TABLE_METADATA;
import static org.opensearch.dataprepper.plugins.source.rds.converter.JoinMetadataEnricher.JOIN_TYPE_METADATA;

class JoinMetadataEnricherTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JoinRelation createRelation(String parent, String child,
                                        Object parentKey, Object childKey,
                                        Object childPrimaryKey, String joinType) {
        try {
            Map<String, Object> map = Map.of(
                    "parent", parent, "child", child,
                    "parent_key", parentKey, "child_key", childKey,
                    "child_primary_key", childPrimaryKey, "join_type", joinType);
            return MAPPER.convertValue(map, JoinRelation.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Event createEvent(Map<String, Object> data) {
        return TestEventFactory.getTestEventFactory().eventBuilder(EventBuilder.class)
                .withEventType("event").withData(data).build();
    }

    @Test
    void enrich_parent_event_sets_correct_metadata() {
        JoinRelation relation = createRelation("orders", "order_items",
                "order_id", "order_id", "item_id", "one_to_many");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        Event event = createEvent(Map.of("order_id", 1, "customer_name", "Alice", "total", 100));
        List<String> columns = List.of("order_id", "customer_name", "total");

        enricher.enrich(event, "orders", columns, false);

        assertThat(event.getMetadata().getAttribute(JOIN_TABLE_METADATA), is("orders"));
        assertThat(event.getMetadata().getAttribute(JOIN_IS_PARENT_METADATA), is("true"));
        assertThat(event.getMetadata().getAttribute(JOIN_IS_DELETE_METADATA), is("false"));
        assertThat(event.getMetadata().getAttribute(JOIN_PRIMARY_KEY_METADATA), is("1"));
        // order_id excluded from fields (it's the join key)
        assertThat(event.getMetadata().getAttribute(JOIN_FIELDS_METADATA), is("customer_name,total"));
        assertThat(event.getMetadata().getAttribute(JOIN_CHILD_PK_VALUE_METADATA), is(""));
    }

    @Test
    void enrich_child_event_sets_correct_metadata() {
        JoinRelation relation = createRelation("orders", "order_items",
                "order_id", "order_id", "item_id", "one_to_many");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        Event event = createEvent(Map.of("item_id", 10, "order_id", 1, "product", "Widget", "price", 50));
        List<String> columns = List.of("item_id", "order_id", "product", "price");

        enricher.enrich(event, "order_items", columns, false);

        assertThat(event.getMetadata().getAttribute(JOIN_TABLE_METADATA), is("order_items"));
        assertThat(event.getMetadata().getAttribute(JOIN_IS_PARENT_METADATA), is("false"));
        assertThat(event.getMetadata().getAttribute(JOIN_PRIMARY_KEY_METADATA), is("1"));
        // order_id and item_id excluded from fields
        assertThat(event.getMetadata().getAttribute(JOIN_FIELDS_METADATA), is("product,price"));
        assertThat(event.getMetadata().getAttribute(JOIN_CHILD_PK_NAME_METADATA), is("item_id"));
        assertThat(event.getMetadata().getAttribute(JOIN_CHILD_PK_VALUE_METADATA), is("10"));
        assertThat(event.getMetadata().getAttribute(JOIN_TYPE_METADATA), is("one_to_many"));
    }

    @Test
    void enrich_composite_key_joins_with_pipe() {
        JoinRelation relation = createRelation("product_catalog", "product_reviews",
                List.of("category", "product_code"), List.of("category", "product_code"),
                "review_id", "one_to_many");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        Event event = createEvent(Map.of("category", "Electronics", "product_code", "LAPTOP-1",
                "name", "Gaming Laptop", "price", 1500));

        enricher.enrich(event, "product_catalog", List.of("category", "product_code", "name", "price"), false);

        assertThat(event.getMetadata().getAttribute(JOIN_PRIMARY_KEY_METADATA), is("Electronics|LAPTOP-1"));
        assertThat(event.getMetadata().getAttribute(JOIN_FIELDS_METADATA), is("name,price"));
    }

    @Test
    void enrich_composite_key_child_sets_composite_pk_value() {
        JoinRelation relation = createRelation("product_catalog", "product_reviews",
                List.of("category", "product_code"), List.of("category", "product_code"),
                "review_id", "one_to_many");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        Event event = createEvent(Map.of("review_id", 5, "category", "Electronics",
                "product_code", "LAPTOP-1", "rating", 5));

        enricher.enrich(event, "product_reviews",
                List.of("review_id", "category", "product_code", "rating"), false);

        assertThat(event.getMetadata().getAttribute(JOIN_PRIMARY_KEY_METADATA), is("Electronics|LAPTOP-1"));
        assertThat(event.getMetadata().getAttribute(JOIN_CHILD_PK_VALUE_METADATA), is("5"));
    }

    @Test
    void enrich_one_to_one_sets_join_type() {
        JoinRelation relation = createRelation("orders", "shipping",
                "order_id", "order_id", "shipping_id", "one_to_one");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        Event event = createEvent(Map.of("shipping_id", 1, "order_id", 5, "carrier", "UPS"));

        enricher.enrich(event, "shipping", List.of("shipping_id", "order_id", "carrier"), false);

        assertThat(event.getMetadata().getAttribute(JOIN_TYPE_METADATA), is("one_to_one"));
    }

    @Test
    void enrich_delete_event_sets_is_delete_true() {
        JoinRelation relation = createRelation("orders", "order_items",
                "order_id", "order_id", "item_id", "one_to_many");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        Event event = createEvent(Map.of("order_id", 1, "customer_name", "Alice"));

        enricher.enrich(event, "orders", List.of("order_id", "customer_name"), true);

        assertThat(event.getMetadata().getAttribute(JOIN_IS_DELETE_METADATA), is("true"));
    }

    @Test
    void isJoinTable_returns_true_for_parent_and_child() {
        JoinRelation relation = createRelation("orders", "order_items",
                "order_id", "order_id", "item_id", "one_to_many");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        assertThat(enricher.isJoinTable("orders"), is(true));
        assertThat(enricher.isJoinTable("order_items"), is(true));
        assertThat(enricher.isJoinTable("other_table"), is(false));
    }

    @Test
    void getChildKeyColumns_returns_columns_for_child_table() {
        JoinRelation relation = createRelation("orders", "order_items",
                "order_id", "order_id", "item_id", "one_to_many");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        assertThat(enricher.getChildKeyColumns("order_items"), equalTo(List.of("order_id")));
        assertThat(enricher.getChildKeyColumns("orders"), is(nullValue()));
        assertThat(enricher.getChildKeyColumns("unknown"), is(nullValue()));
    }

    @Test
    void getChildKeyColumns_returns_composite_columns() {
        JoinRelation relation = createRelation("product_catalog", "product_reviews",
                List.of("category", "product_code"), List.of("category", "product_code"),
                "review_id", "one_to_many");
        JoinMetadataEnricher enricher = new JoinMetadataEnricher(List.of(relation));

        assertThat(enricher.getChildKeyColumns("product_reviews"),
                equalTo(List.of("category", "product_code")));
    }

    @Test
    void test_isKeyLengthValid_equal_length() {
        JoinRelation relation = createRelation("orders", "items",
                List.of("tenant_id", "order_id"), List.of("tenant_id", "order_id"),
                "item_id", "one_to_many");
        assertThat(relation.isKeyLengthValid(), is(true));
    }

    @Test
    void test_isKeyLengthValid_unequal_length() {
        JoinRelation relation = createRelation("orders", "items",
                List.of("tenant_id", "order_id"), List.of("order_id"),
                "item_id", "one_to_many");
        assertThat(relation.isKeyLengthValid(), is(false));
    }
}
