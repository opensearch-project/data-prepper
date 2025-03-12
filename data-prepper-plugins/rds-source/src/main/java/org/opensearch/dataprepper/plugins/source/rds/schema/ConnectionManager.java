/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for managing connections to a database.
 */
public interface ConnectionManager {

    Connection getConnection() throws SQLException;
}
