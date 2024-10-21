/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.resync;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyAction;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyRelation;
import org.opensearch.dataprepper.plugins.source.rds.model.ParentTable;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CascadingActionDetectorTest {
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Event event;

    @Mock
    private TableMetadata tableMetadata;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private StreamPartition streamPartition;

    private CascadingActionDetector objectUnderTest;
    private ForeignKeyRelation foreignKeyRelationWithCascading;
    private ForeignKeyRelation foreignKeyRelationWithoutCascading;
    private Map<String, ParentTable> parentTableMap;

    @BeforeEach
    void setUp() {
        objectUnderTest = createObjectUnderTest();
        prepareTestTables();
    }

    @Test
    void testGetParentTableMap_returns_empty_list_if_stream_progress_state_is_empty() {
        when(streamPartition.getProgressState()).thenReturn(Optional.empty());

        Map<String, ParentTable> actualParentTableMap = objectUnderTest.getParentTableMap(streamPartition);

        assertThat(actualParentTableMap.size(), is(0));
    }

    @Test
    void testGetParentTableMap_returns_only_foreign_relations_with_cascading_actions() {
        final StreamProgressState progressState = mock(StreamProgressState.class);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(progressState));
        when(progressState.getForeignKeyRelations()).thenReturn(List.of(foreignKeyRelationWithCascading, foreignKeyRelationWithoutCascading));

        Map<String, ParentTable> actualParentTableMap = objectUnderTest.getParentTableMap(streamPartition);

        assertThat(actualParentTableMap.size(), is(1));
        assertThat(actualParentTableMap.containsKey("test-database.parent-table1"), is(true));

        final ParentTable parentTable = actualParentTableMap.get("test-database.parent-table1");
        assertThat(parentTable.getDatabaseName(), is("test-database"));
        assertThat(parentTable.getTableName(), is("parent-table1"));
        assertThat(parentTable.getReferencedColumnMap().size(), is(1));
        assertThat(parentTable.getReferencedColumnMap().containsKey("referenced-column"), is(true));
        assertThat(parentTable.getReferencedColumnMap().get("referenced-column").size(), is(1));
        assertThat(parentTable.getReferencedColumnMap().get("referenced-column").get(0), is(foreignKeyRelationWithCascading));
    }

    @Test
    void testDetectCascadingUpdates() {
        UpdateRowsEventData data = mock(UpdateRowsEventData.class);
        List<Map.Entry<Serializable[], Serializable[]>> rows = List.of(Map.entry(new Serializable[]{"old-value"}, new Serializable[]{"new-value"}));
        long timestampInMillis = Instant.now().toEpochMilli();
        List<String> primaryKeys = List.of("primary-key");
        when(event.getData()).thenReturn(data);
        when(event.getHeader().getTimestamp()).thenReturn(timestampInMillis);
        when(tableMetadata.getFullTableName()).thenReturn("test-database.parent-table1");
        when(data.getRows()).thenReturn(rows);
        when(tableMetadata.getColumnNames()).thenReturn(List.of("referenced-column"));
        when(tableMetadata.getPrimaryKeys()).thenReturn(primaryKeys);

        objectUnderTest.detectCascadingUpdates(event, parentTableMap, tableMetadata);

        ArgumentCaptor<ResyncPartition> resyncPartitionArgumentCaptor = ArgumentCaptor.forClass(ResyncPartition.class);
        verify(sourceCoordinator).createPartition(resyncPartitionArgumentCaptor.capture());
        ResyncPartition resyncPartition = resyncPartitionArgumentCaptor.getValue();

        assertThat(resyncPartition.getPartitionKey(), is("test-database|child-table|" + timestampInMillis));

        ResyncProgressState progressState = resyncPartition.getProgressState().get();
        assertThat(progressState.getForeignKeyName(), is("foreign-key1"));
        assertThat(progressState.getUpdatedValue(), is("new-value"));
        assertThat(progressState.getPrimaryKeys(), is(primaryKeys));
    }

    @Test
    void detectCascadingDeletes() {
        long timestampInMillis = Instant.now().toEpochMilli();
        List<String> primaryKeys = List.of("primary-key");
        when(event.getHeader().getTimestamp()).thenReturn(timestampInMillis);
        when(tableMetadata.getFullTableName()).thenReturn("test-database.parent-table1");
        when(tableMetadata.getPrimaryKeys()).thenReturn(primaryKeys);

        objectUnderTest.detectCascadingDeletes(event, parentTableMap, tableMetadata);

        ArgumentCaptor<ResyncPartition> resyncPartitionArgumentCaptor = ArgumentCaptor.forClass(ResyncPartition.class);
        verify(sourceCoordinator).createPartition(resyncPartitionArgumentCaptor.capture());
        ResyncPartition resyncPartition = resyncPartitionArgumentCaptor.getValue();

        assertThat(resyncPartition.getPartitionKey(), is("test-database|child-table|" + timestampInMillis));

        ResyncProgressState progressState = resyncPartition.getProgressState().get();
        assertThat(progressState.getForeignKeyName(), is("foreign-key1"));
        assertThat(progressState.getUpdatedValue(), nullValue());
        assertThat(progressState.getPrimaryKeys(), is(primaryKeys));
    }

    private CascadingActionDetector createObjectUnderTest() {
        return new CascadingActionDetector(sourceCoordinator);
    }

    private void prepareTestTables() {
        final String databaseName = "test-database";
        final String parentTableName1 = "parent-table1";
        final String parentTableName2 = "parent-table2";
        final String referencedColumnName = "referenced-column";
        final String childTableName = "child-table";
        final String foreignKey1 = "foreign-key1";
        final String foreignKey2 = "foreign-key2";


        foreignKeyRelationWithCascading = ForeignKeyRelation.builder()
                .databaseName(databaseName)
                .parentTableName(parentTableName1)
                .referencedKeyName(referencedColumnName)
                .childTableName(childTableName)
                .foreignKeyName(foreignKey1)
                .updateAction(ForeignKeyAction.CASCADE)
                .deleteAction(ForeignKeyAction.SET_NULL)
                .build();

        foreignKeyRelationWithoutCascading = ForeignKeyRelation.builder()
                .databaseName(databaseName)
                .parentTableName(parentTableName2)
                .referencedKeyName(referencedColumnName)
                .childTableName(childTableName)
                .foreignKeyName(foreignKey2)
                .updateAction(ForeignKeyAction.RESTRICT)
                .build();

        ParentTable parentTable1 = ParentTable.builder()
                .databaseName(databaseName)
                .tableName(parentTableName1)
                .referencedColumnMap(Map.of(referencedColumnName, List.of(foreignKeyRelationWithCascading)))
                .build();

        parentTableMap = Map.of(databaseName + "." + parentTableName1, parentTable1);
    }
}