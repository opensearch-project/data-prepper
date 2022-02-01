/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
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

    @JsonProperty("overwrite_on_rename")
    final private boolean overwriteOnRename = true;

    @JsonProperty("overwrite_on_add")
    final private boolean overwriteOnAdd = true;

    @JsonProperty("overwrite_on_copy")
    final private boolean overwriteOnCopy = true;

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

    public boolean getOverwriteOnRename() {
        return overwriteOnRename;
    }

    public boolean getOverwriteOnAdd() {
        return overwriteOnAdd;
    }

    public boolean getOverwriteOnCopy() {
        return overwriteOnCopy;
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
