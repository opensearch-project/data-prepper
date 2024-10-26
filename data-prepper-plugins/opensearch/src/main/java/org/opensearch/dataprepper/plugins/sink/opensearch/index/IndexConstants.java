/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexConstants {
  public static final Set<String> TYPES = new HashSet<>();
  public static final Map<IndexType, String> TYPE_TO_DEFAULT_ALIAS = new HashMap<>();
  // TODO: extract out version number into version enum
  public static final String RAW_DEFAULT_TEMPLATE_FILE = "otel-v1-apm-span-index-template.json";
  public static final String RAW_ISM_POLICY = "raw-span-policy";
  public static final String RAW_ISM_FILE_NO_ISM_TEMPLATE = "raw-span-policy-no-ism-template.json";
  public static final String RAW_ISM_FILE_WITH_ISM_TEMPLATE = "raw-span-policy-with-ism-template.json";
  public static final String ISM_ENABLED_SETTING = "opendistro.index_state_management.enabled";
  public static final String ISM_POLICY_ID_SETTING = "opendistro.index_state_management.policy_id";
  public static final String ISM_ROLLOVER_ALIAS_SETTING = "opendistro.index_state_management.rollover_alias";
  public static final String PLUGINS_ROLLOVER_ALIAS_SETTING = "plugins.index_state_management.rollover_alias";
  // TODO: extract out version number into version enum
  public static final String SERVICE_MAP_DEFAULT_TEMPLATE_FILE = "otel-v1-apm-service-map-index-template.json";

  static {
    // TODO: extract out version number into version enum
    TYPE_TO_DEFAULT_ALIAS.put(IndexType.TRACE_ANALYTICS_RAW, "otel-v1-apm-span");
    TYPE_TO_DEFAULT_ALIAS.put(IndexType.TRACE_ANALYTICS_SERVICE_MAP, "otel-v1-apm-service-map");
  }
}
