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

public class QueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);

    private final ConnectionManager connectionManager;

    public QueryManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public List<Map<String, Object>> selectRows(String query) {
        List<Map<String, Object>> result = new ArrayList<>();
        try (final Connection connection = connectionManager.getConnection()) {
            final Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            return convertResultSetToList(resultSet);
        } catch (Exception e) {
            LOG.error("Failed to execute query {}, retrying", query, e);
            return result;
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
}
