package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class PostgresSchemaManager implements SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaManager.class);
    private final PostgresConnectionManager connectionManager;

    public PostgresSchemaManager(PostgresConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public void createLogicalReplicationSlot(final List<String> tableNames, final String publicationName, final String slotName) {
        StringBuilder createPublicationStatementBuilder = new StringBuilder("CREATE PUBLICATION ")
                .append(publicationName)
                .append(" FOR TABLE ");
        for (int i = 0; i < tableNames.size(); i++) {
            createPublicationStatementBuilder.append(tableNames.get(i));
            if (i < tableNames.size() - 1) {
                createPublicationStatementBuilder.append(", ");
            }
        }
        createPublicationStatementBuilder.append(";");
        final String createPublicationStatement = createPublicationStatementBuilder.toString();

        try (Connection conn = connectionManager.getConnection()) {
            try {
                PreparedStatement statement = conn.prepareStatement(createPublicationStatement);
                statement.executeUpdate();
            } catch (Exception e) {
                LOG.info("Failed to create publication: {}", e.getMessage());
            }

            PGConnection pgConnection = conn.unwrap(PGConnection.class);

            // Create replication slot
            PGReplicationConnection replicationConnection = pgConnection.getReplicationAPI();
            try {
                replicationConnection.createReplicationSlot()
                        .logical()
                        .withSlotName(slotName)
                        .withOutputPlugin("pgoutput")
                        .make();
                LOG.info("Replication slot created successfully.");
            } catch (Exception e) {
                LOG.info("Failed to create replication slot: {}", e.getMessage());
            }
        } catch (Exception e) {
            LOG.error("Exception when creating replication slot. ", e);
        }
    }
}
