/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.sql.DatabaseMetaData;
import java.util.Set;

public enum ForeignKeyAction {
    CASCADE,
    NO_ACTION,
    RESTRICT,
    SET_DEFAULT,
    SET_NULL,
    UNKNOWN;

    /**
     * Returns the corresponding ForeignKeyAction for the given metadata action value.
     *
     * @param action the metadata action value
     * @return the corresponding ForeignKeyAction
     */
    public static ForeignKeyAction getActionFromMetadata(short action) {
        switch (action) {
            case DatabaseMetaData.importedKeyCascade:
                return CASCADE;
            case DatabaseMetaData.importedKeySetNull:
                return SET_NULL;
            case DatabaseMetaData.importedKeySetDefault:
                return SET_DEFAULT;
            case DatabaseMetaData.importedKeyRestrict:
                return RESTRICT;
            case DatabaseMetaData.importedKeyNoAction:
                return NO_ACTION;
            default:
                return UNKNOWN;
        }
    }

    /**
     * Checks if the foreign key action is one of the cascading actions (CASCADE, SET_DEFAULT, SET_NULL)
     * that will result in changes to the foreign key value when referenced key in parent table changes.
     *
     * @param foreignKeyAction the foreign key action
     * @return true if the foreign key action is a cascade action, false otherwise
     */
    public static boolean isCascadingAction(ForeignKeyAction foreignKeyAction) {
        if (foreignKeyAction == null) {
            return false;
        }
        return Set.of(CASCADE, SET_DEFAULT, SET_NULL).contains(foreignKeyAction);
    }
}
