/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import jakarta.validation.constraints.AssertFalse;

import java.lang.reflect.Field;
import java.util.Map;

public class MutateEventProcessorConfig {
    @MutateAction
    final private Map<String, String> rename = null;
    @MutateAction
    final private Map<String, Object> add = null;
    @MutateAction
    final private String delete = null;
    @MutateAction
    final private Map<String, String> copy = null;

    final private boolean overwrite = true;

    public Map<String, String> getRename() {
        return rename;
    }

    public Map<String, Object> getAdd() {
        return add;
    }

    public String getDelete() {
        return delete;
    }

    public Map<String, String> getCopy() {
        return copy;
    }

    public boolean getOverwrite() {
        return overwrite;
    }

    @AssertFalse(message="More than one mutation action defined. Only one mutation action allowed.")
    public boolean isMoreThanOneMutationActionDefined() {
        return getNumberOfMutationActionsDefined() > 1;
    }

    @AssertFalse(message="No mutation actions defined. A mutation action must be defined.")
    public boolean isNoMutationActionDefined() {
        return getNumberOfMutationActionsDefined() == 0;
    }

    public int getNumberOfMutationActionsDefined() {
        final Field[] fields = this.getClass().getDeclaredFields();
        int definedCount = 0;

        for(Field field : fields) {
            try {
                if (field.isAnnotationPresent(MutateAction.class)
                        && field.get(this) != null) {
                    definedCount++;
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return definedCount;
    }
}
