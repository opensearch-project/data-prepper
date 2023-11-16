package org.opensearch.dataprepper.plugins.source.dynamodb.utils;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class TableUtilTest {

    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    @Test
    void test_getTableNameFromArn_should_return_tableName() {
        String result = TableUtil.getTableNameFromArn(tableArn);
        assertThat(result, equalTo(tableName));
    }

    @Test
    void test_getTableArnFromStreamArn_should_return_tableArn() {
        String result = TableUtil.getTableArnFromStreamArn(streamArn);
        assertThat(result, equalTo(tableArn));
    }

    @Test
    void test_getTableArnFromExportArn_should_return_tableArn() {
        String result = TableUtil.getTableArnFromExportArn(exportArn);
        assertThat(result, equalTo(tableArn));
    }
}