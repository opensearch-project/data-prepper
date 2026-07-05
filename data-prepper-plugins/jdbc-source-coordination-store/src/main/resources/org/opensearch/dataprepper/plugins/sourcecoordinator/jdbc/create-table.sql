-- Copyright OpenSearch Contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- The OpenSearch Contributors require contributions made to
-- this file be licensed under the Apache-2.0 license or a
-- compatible open source license.

CREATE TABLE IF NOT EXISTS %s (
    source_identifier VARCHAR(256) NOT NULL,
    source_partition_key VARCHAR(512) NOT NULL,
    partition_owner VARCHAR(256),
    partition_progress_state TEXT,
    source_partition_status VARCHAR(20) NOT NULL,
    partition_ownership_timeout TIMESTAMP,
    reopen_at TIMESTAMP,
    closed_count BIGINT DEFAULT 0,
    partition_priority VARCHAR(64),
    version BIGINT NOT NULL DEFAULT 0,
    expiration_time TIMESTAMP,
    PRIMARY KEY (source_identifier, source_partition_key)
)
