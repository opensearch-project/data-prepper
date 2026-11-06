-- Copyright OpenSearch Contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- The OpenSearch Contributors require contributions made to
-- this file be licensed under the Apache-2.0 license or a
-- compatible open source license.

CREATE INDEX idx_source_status_priority ON %s (source_identifier, source_partition_status, partition_priority)
