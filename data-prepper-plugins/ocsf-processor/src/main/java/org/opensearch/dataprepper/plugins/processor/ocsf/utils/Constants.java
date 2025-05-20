/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.ocsf.utils;

public final class Constants {
    // Main schema sections
    public static final String MAPPING_OPTIONS = "mapping_options";
    public static final String MAPPING_SELECTOR = "mapping_selector";
    public static final String DIRECT_MAPPINGS = "direct_mappings";
    public static final String TRANSFORMATIONS = "transformations";

    // Common mapping fields
    public static final String SOURCE = "source";
    public static final String DESTINATION = "destination";
    public static final String TYPE = "type";
    public static final String TRANSFORMATION = "transformation";
    public static final String OPERATION = "operation";
    public static final String VALUE = "value";
    public static final String CASES = "cases";
    public static final String CONDITION = "condition";
    public static final String OPERATIONS = "operations";
    public static final String DEFAULT = "default";
    public static final String LHS = "lhs";
    public static final String RHS = "rhs";

    // Standard schema fields
    public static final String OPERATION_SCHEMA_VALIDATON = "Operation";
    public static final String WORKLOAD = "Workload";
    public static final String CREATION_TIME = "CreationTime";

    // OCSF schema fields
    public static final String SOFTWARE_TYPE = "SoftwareType";
    public static final String COMPANY_NAME = "CompanyName";
}