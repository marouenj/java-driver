/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.*;

import java.util.List;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.Metadata.handleId;
import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@CCMConfig(createCluster = false)
public class SchemaChangesTest extends CCMTestsSupport {

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }";
    private static final String ALTER_KEYSPACE = "ALTER KEYSPACE %s WITH durable_writes = false";
    private static final String DROP_KEYSPACE = "DROP KEYSPACE %s";

    private static final String CREATE_TABLE = "CREATE TABLE %s.table1(i int primary key)";
    private static final String ALTER_TABLE = "ALTER TABLE %s.table1 ADD j int";
    private static final String DROP_TABLE = "DROP TABLE %s.table1";

    /**
     * The maximum time that the test will wait to check that listeners have been notified
     */
    private static final int NOTIF_TIMEOUT_MS = 120000;

    Cluster cluster1;
    Cluster cluster2; // a second cluster to check that other clients also get notified
    Cluster schemaDisabledCluster; // a cluster with schema metadata disabled.

    Session session1;

    Session schemaDisabledSession;

    SchemaChangeListener listener1;
    SchemaChangeListener listener2;
    SchemaChangeListener schemaDisabledListener;

    List<SchemaChangeListener> listeners;

    ControlConnection schemaDisabledControlConnection;

    @BeforeClass(groups = "short")
    public void setup() throws InterruptedException {
        Cluster.Builder builder = Cluster.builder()
                .addContactPointsWithPorts(getInitialContactPoints())
                .withQueryOptions(nonDebouncingQueryOptions());
        cluster1 = builder.build();
        cluster2 = builder.build();
        schemaDisabledCluster = spy(Cluster.builder()
                .addContactPointsWithPorts(getInitialContactPoints())
                .withClusterName("schema-disabled")
                .withQueryOptions(nonDebouncingQueryOptions()
                                .setMetadataEnabled(false)
                ).build());

        schemaDisabledSession = schemaDisabledCluster.connect();

        schemaDisabledControlConnection = spy(schemaDisabledCluster.manager.controlConnection);
        schemaDisabledCluster.manager.controlConnection = schemaDisabledControlConnection;

        session1 = cluster1.connect();
        cluster2.init();

        cluster1.register(listener1 = mock(SchemaChangeListener.class));
        cluster1.register(listener2 = mock(SchemaChangeListener.class));
        listeners = Lists.newArrayList(listener1, listener2);

        schemaDisabledCluster.register(schemaDisabledListener = mock(SchemaChangeListener.class));
        verify(schemaDisabledListener, times(1)).onRegister(schemaDisabledCluster);

        execute(CREATE_KEYSPACE, "lowercase");
        execute(CREATE_KEYSPACE, "\"CaseSensitive\"");
    }

    @AfterClass(groups = "short", alwaysRun = true)
    public void teardown() {
        if (cluster1 != null)
            cluster1.close();
        if (cluster2 != null)
            cluster2.close();
        if (schemaDisabledCluster != null)
            schemaDisabledCluster.close();
    }


    @DataProvider(name = "existingKeyspaceName")
    public static Object[][] existingKeyspaceName() {
        return new Object[][]{{"lowercase"}, {"\"CaseSensitive\""}};
    }

    @DataProvider(name = "newKeyspaceName")
    public static Object[][] newKeyspaceName() {
        return new Object[][]{{"lowercase2"}, {"\"CaseSensitive2\""}};
    }

    @BeforeMethod(groups = "short")
    public void resetListeners() {
        for (SchemaChangeListener listener : listeners) {
            reset(listener);
        }
        reset(schemaDisabledControlConnection);
    }

    /**
     * Ensures that a listener registered on a Cluster that has schema metadata disabled
     * is never invoked with schema change events.
     *
     * @jira_ticket JAVA-858
     * @since 2.0.11
     */
    @AfterMethod(groups = "short")
    public void verifyNoMoreInteractionsWithListener() {
        verifyNoMoreInteractions(schemaDisabledListener);
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_creation(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableAdded(added.capture());
            assertThat(added.getValue())
                    .isInKeyspace(handleId(keyspace))
                    .hasName("table1");
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getKeyspace(keyspace).getTable("table1")).isNotNull();
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_update(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace);
        ArgumentCaptor<TableMetadata> added = null;
        for (SchemaChangeListener listener : listeners) {
            added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableAdded(added.capture());
            assertThat(added.getValue())
                    .isInKeyspace(handleId(keyspace))
                    .hasName("table1");
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getKeyspace(keyspace).getTable("table1")).hasNoColumn("j");
        assert added != null;
        execute(ALTER_TABLE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> current = ArgumentCaptor.forClass(TableMetadata.class);
            ArgumentCaptor<TableMetadata> previous = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableChanged(current.capture(), previous.capture());
            assertThat(previous.getValue())
                    .isEqualTo(added.getValue())
                    .hasNoColumn("j");
            assertThat(current.getValue())
                    .isInKeyspace(handleId(keyspace))
                    .hasName("table1")
                    .hasColumn("j");
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getKeyspace(keyspace).getTable("table1")).hasColumn("j");
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_drop(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableAdded(added.capture());
            assertThat(added.getValue())
                    .hasName("table1")
                    .isInKeyspace(handleId(keyspace));
        }
        refreshSchema();
        execute(DROP_TABLE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> removed = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableRemoved(removed.capture());
            assertThat(removed.getValue()).hasName("table1");
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getKeyspace(keyspace).getTable("table1")).isNull();
    }

    @SuppressWarnings("RedundantCast")
    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    @CassandraVersion(major = 2.1)
    public void should_notify_of_udt_creation(String keyspace) {
        session1.execute(String.format("CREATE TYPE %s.type1(i int)", keyspace));
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<UserType> added = ArgumentCaptor.forClass(UserType.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onUserTypeAdded(added.capture());
            assertThat((DataType) added.getValue())
                    .isUserType(handleId(keyspace), "type1");
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat((DataType) m.getKeyspace(keyspace).getUserType("type1")).isNotNull();
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    @CassandraVersion(major = 2.1)
    public void should_notify_of_udt_update(String keyspace) {
        session1.execute(String.format("CREATE TYPE %s.type1(i int)", keyspace));
        session1.execute(String.format("ALTER TYPE %s.type1 ADD j int", keyspace));
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<UserType> current = ArgumentCaptor.forClass(UserType.class);
            ArgumentCaptor<UserType> previous = ArgumentCaptor.forClass(UserType.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onUserTypeChanged(current.capture(), previous.capture());
            assertThat(previous.getValue().getFieldNames()).doesNotContain("j");
            assertThat(current.getValue().getFieldNames()).contains("j");
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getKeyspace(keyspace).getUserType("type1").getFieldType("j")).isNotNull();
    }

    @SuppressWarnings("RedundantCast")
    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    @CassandraVersion(major = 2.1)
    public void should_notify_of_udt_drop(String keyspace) {
        session1.execute(String.format("CREATE TYPE %s.type1(i int)", keyspace));
        session1.execute(String.format("DROP TYPE %s.type1", keyspace));
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<UserType> removed = ArgumentCaptor.forClass(UserType.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onUserTypeRemoved(removed.capture());
            assertThat((DataType) removed.getValue()).isUserType(handleId(keyspace), "type1");
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat((DataType) m.getKeyspace(keyspace).getUserType("type1")).isNull();
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_creation(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<KeyspaceMetadata> added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceAdded(added.capture());
            assertThat(added.getValue()).hasName(handleId(keyspace));
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getKeyspace(keyspace)).isNotNull();
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_update(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace);
        ArgumentCaptor<KeyspaceMetadata> added = null;
        for (SchemaChangeListener listener : listeners) {
            added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceAdded(added.capture());
            assertThat(added.getValue()).hasName(handleId(keyspace));
        }
        assert added != null;
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getKeyspace(keyspace).isDurableWrites()).isTrue();
        execute(ALTER_KEYSPACE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<KeyspaceMetadata> current = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            ArgumentCaptor<KeyspaceMetadata> previous = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceChanged(current.capture(), previous.capture());
            assertThat(previous.getValue())
                    .isEqualTo(added.getValue())
                    .isDurableWrites();
            assertThat(current.getValue())
                    .hasName(handleId(keyspace))
                    .isNotDurableWrites();
        }
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getKeyspace(keyspace)).isNotDurableWrites();
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_drop(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace);
        ArgumentCaptor<KeyspaceMetadata> added = null;
        for (SchemaChangeListener listener : listeners) {
            added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceAdded(added.capture());
            assertThat(added.getValue()).hasName(handleId(keyspace));
        }
        assert added != null;
        refreshSchema();
        for (Metadata m : metadatas())
            assertThat(m.getReplicas(keyspace, Bytes.fromHexString("0xCAFEBABE"))).isNotEmpty();
        execute(CREATE_TABLE, keyspace); // to test table drop notifications
        execute(DROP_KEYSPACE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> table = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableRemoved(table.capture());
            assertThat(table.getValue())
                    .hasName("table1")
                    .isInKeyspace(handleId(keyspace));
            ArgumentCaptor<KeyspaceMetadata> ks = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceRemoved(ks.capture());
            assertThat(ks.getValue())
                    .hasName(handleId(keyspace));
        }
        refreshSchema();
        for (Metadata m : metadatas()) {
            assertThat(m.getKeyspace(keyspace)).isNull();
            assertThat(m.getReplicas(keyspace, Bytes.fromHexString("0xCAFEBABE"))).isEmpty();
        }
    }


    /**
     * Ensures that calling {@link Metadata#newToken(String)} on a Cluster that has schema
     * metadata disabled will throw a {@link IllegalStateException}.
     *
     * @jira_ticket JAVA-858
     * @since 2.0.11
     */
    @Test(groups = "short", expectedExceptions = IllegalStateException.class)
    public void should_throw_illegal_state_exception_on_newToken_with_metadata_disabled() {
        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(getInitialContactPoints())
                .withQueryOptions(nonDebouncingQueryOptions()
                                .setMetadataEnabled(false)
                ).build();

        try {
            cluster.init();
            cluster.getMetadata().newToken("0x00");
        } finally {
            cluster.close();
        }
    }


    /**
     * Ensures that calling {@link Metadata#newTokenRange(Token, Token)} on a Cluster that has schema
     * metadata disabled will throw a {@link IllegalStateException}.
     *
     * @jira_ticket JAVA-858
     * @since 2.0.11
     */
    @Test(groups = "short", expectedExceptions = IllegalStateException.class)
    public void should_throw_illegal_state_exception_on_newTokenRange_with_metadata_disabled() {
        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(getInitialContactPoints())
                .withQueryOptions(nonDebouncingQueryOptions()
                                .setMetadataEnabled(false)
                ).build();

        try {
            cluster.init();
            Token.Factory factory = Token.getFactory("Murmur3Partitioner");
            Token token = factory.fromString(Long.toString(1));
            cluster.getMetadata().newTokenRange(token, token);
        } finally {
            cluster.close();
        }
    }

    /**
     * Ensures that executing a query causing a schema change with a Cluster that has schema metadata
     * disabled will still wait on schema agreement, but not refresh the schema.
     *
     * @jira_ticket JAVA-858
     * @since 2.0.11
     */
    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_not_refresh_schema_on_schema_change_response(String keyspace) throws InterruptedException {
        ResultSet rs = schemaDisabledSession.execute(String.format(CREATE_TABLE, keyspace));

        // Should still wait on schema agreement.
        assertThat(rs.getExecutionInfo().isSchemaInAgreement()).isTrue();
        assertThat(schemaDisabledCluster.getMetadata().checkSchemaAgreement()).isTrue();

        // Wait up to 1 second (since refreshSchema submitted in an executor) and check that refreshSchema never called.
        verify(schemaDisabledControlConnection, after(1000).never()).refreshSchema(any(SchemaElement.class), any(String.class), any(String.class));
    }

    /**
     * Ensures that when schema metadata is enabled using {@link QueryOptions#setMetadataEnabled(boolean)}
     * that a schema and nodelist refresh is submitted, but only if schema metadata is currently disabled.
     *
     * @jira_ticket JAVA-858
     * @since 2.0.11
     */
    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_refresh_schema_and_token_map_if_schema_metadata_reenabled(String keyspace) throws Exception {
        try {
            schemaDisabledCluster.getConfiguration().getQueryOptions().setMetadataEnabled(true);

            verify(schemaDisabledControlConnection, after(1000)).refreshSchema(null, null, null);
            verify(schemaDisabledControlConnection).refreshNodeListAndTokenMap();

            // Ensure that there is schema metadata.
            assertThat(schemaDisabledCluster.getMetadata().getKeyspace(keyspace)).isNotNull();
            Token token1 = schemaDisabledCluster.getMetadata().newToken("0");
            Token token2 = schemaDisabledCluster.getMetadata().newToken("111111");
            assertThat(token1).isNotNull();
            assertThat(token2).isNotNull();
            assertThat(schemaDisabledCluster.getMetadata().newTokenRange(token1, token2)).isNotNull();

            assertThat(schemaDisabledCluster.getMetadata().getTokenRanges()).isNotNull().isNotEmpty();

            // Try enabling again and ensure schema is not refreshed again.
            reset(schemaDisabledControlConnection);
            schemaDisabledCluster.getConfiguration().getQueryOptions().setMetadataEnabled(true);
            verify(schemaDisabledControlConnection, after(1000).never()).refreshSchema(null, null, null);
            verify(schemaDisabledControlConnection, never()).refreshNodeListAndTokenMap();
        } finally {
            // Reset listener mock to not count it's interactions in this test.
            reset(schemaDisabledListener);
            schemaDisabledCluster.getConfiguration().getQueryOptions().setMetadataEnabled(false);
        }
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void cleanup() throws InterruptedException {
        ListenableFuture<List<ResultSet>> f = Futures.successfulAsList(Lists.newArrayList(
                session1.executeAsync("DROP TABLE lowercase.table1"),
                session1.executeAsync("DROP TABLE \"CaseSensitive\".table1"),
                session1.executeAsync("DROP TYPE lowercase.type1"),
                session1.executeAsync("DROP TYPE \"CaseSensitive\".type1"),
                session1.executeAsync("DROP KEYSPACE lowercase2"),
                session1.executeAsync("DROP KEYSPACE \"CaseSensitive2\"")
        ));
        Futures.getUnchecked(f);
    }

    private void execute(String cql, String keyspace) throws InterruptedException {
        session1.execute(String.format(cql, keyspace));
    }

    private List<Metadata> metadatas() {
        return Lists.newArrayList(cluster1.getMetadata(), cluster2.getMetadata());
    }

    @SuppressWarnings("unchecked")
    private void refreshSchema() {
        Futures.getUnchecked(
                Futures.successfulAsList(
                        cluster1.manager.submitSchemaRefresh(null, null, null),
                        cluster2.manager.submitSchemaRefresh(null, null, null)));
    }


}
