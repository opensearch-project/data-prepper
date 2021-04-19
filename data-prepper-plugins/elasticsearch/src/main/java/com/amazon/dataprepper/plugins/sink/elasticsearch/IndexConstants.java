/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.elasticsearch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexConstants {
  public static final String RAW = "raw";
  public static final String SERVICE_MAP = "service_map";
  public static final String CUSTOM = "custom";
  public static final Set<String> TYPES = new HashSet<>();
  public static final Map<String, String> TYPE_TO_DEFAULT_ALIAS = new HashMap<>();
  // TODO: extract out version number into version enum
  public static final String RAW_DEFAULT_TEMPLATE_FILE = "otel-v1-apm-span-index-template.json";
  public static final String RAW_ISM_POLICY = "raw-span-policy";
  public static final String RAW_ISM_FILE = "raw-span-policy.json";
  public static final String ISM_ENABLED_SETTING = "opendistro.index_state_management.enabled";
  public static final String ISM_POLICY_ID_SETTING = "opendistro.index_state_management.policy_id";
  public static final String ISM_ROLLOVER_ALIAS_SETTING = "opendistro.index_state_management.rollover_alias";
  // TODO: extract out version number into version enum
  public static final String SERVICE_MAP_DEFAULT_TEMPLATE_FILE = "otel-v1-apm-service-map-index-template.json";

  static {
    TYPES.add(RAW);
    TYPES.add(SERVICE_MAP);
    TYPES.add(CUSTOM);
    // TODO: extract out version number into version enum
    TYPE_TO_DEFAULT_ALIAS.put(RAW, "otel-v1-apm-span");
    TYPE_TO_DEFAULT_ALIAS.put(SERVICE_MAP, "otel-v1-apm-service-map");
  }
}
