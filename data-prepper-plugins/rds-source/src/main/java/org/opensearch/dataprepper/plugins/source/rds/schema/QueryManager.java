/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class QueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);

    static final int NUM_OF_RETRIES = 3;
    static final int BACKOFF_IN_MILLIS = 500;

    private final ConnectionManager connectionManager;

    public QueryManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public List<Map<String, Object>> selectRows(String query) {
        return executeWithRetry(this::doSelectRows, query);
    }

    private List<Map<String, Object>> doSelectRows(String query) {
        List<Map<String, Object>> result = new ArrayList<>();
        try (final Connection connection = connectionManager.getConnection()) {
            final Statement statement = connection.createStatement();
            try (ResultSet resultSet = statement.executeQuery(query)) {
                return convertResultSetToList(resultSet);
            }
        } catch (Exception e) {
            LOG.error("Failed to execute query {}, retrying", query);
            throw new RuntimeException(e);
        }
    }

    private List<Map<String, Object>> convertResultSetToList(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<Map<String, Object>> result = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                row.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
            result.add(row);
        }
        return result;
    }

    private <T, R> R executeWithRetry(Function<T, R> function, T query) {
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            try {
                return function.apply(query);
            } catch (Exception e) {
                applyBackoff();
            }
            retry++;
        }
        throw new RuntimeException("Failed to execute query after " + NUM_OF_RETRIES + " retries");
    }

    private void applyBackoff() {
        try {
            Thread.sleep(BACKOFF_IN_MILLIS);
        } catch (final InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }
}
