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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.assertEquals;

/**
 * Test we correctly process and print schema.
 */
@CCMConfig(clusterProvider = "createClusterBuilderNoDebouncing")
public class SchemaTest extends CCMTestsSupport {
    static final Logger logger = LoggerFactory.getLogger(SchemaTest.class);

    private static final Map<String, String> cql3 = new HashMap<String, String>();
    private static final Map<String, String> compact = new HashMap<String, String>();

    private static String withOptions;

    @Override
    public Collection<String> createTestFixtures() {

        String sparse = String.format("CREATE TABLE %s.sparse (\n"
                + "    k text,\n"
                + "    c1 int,\n"
                + "    c2 float,\n"
                + "    l list<text>,\n"
                + "    v int,\n"
                + "    PRIMARY KEY (k, c1, c2)\n"
                + ");", keyspace);

        String st = String.format("CREATE TABLE %s.static (\n"
                + "    k text,\n"
                + "    i int,\n"
                + "    m map<text, timeuuid>,\n"
                + "    v int,\n"
                + "    PRIMARY KEY (k)\n"
                + ");", keyspace);

        String counters = String.format("CREATE TABLE %s.counters (\n"
                + "    k text,\n"
                + "    c counter,\n"
                + "    PRIMARY KEY (k)\n"
                + ");", keyspace);

        String compactStatic = String.format("CREATE TABLE %s.compact_static (\n"
                + "    k text,\n"
                + "    i int,\n"
                + "    t timeuuid,\n"
                + "    v int,\n"
                + "    PRIMARY KEY (k)\n"
                + ") WITH COMPACT STORAGE;", keyspace);

        String compactDynamic = String.format("CREATE TABLE %s.compact_dynamic (\n"
                + "    k text,\n"
                + "    c int,\n"
                + "    v timeuuid,\n"
                + "    PRIMARY KEY (k, c)\n"
                + ") WITH COMPACT STORAGE;", keyspace);

        String compactComposite = String.format("CREATE TABLE %s.compact_composite (\n"
                + "    k text,\n"
                + "    c1 int,\n"
                + "    c2 float,\n"
                + "    c3 double,\n"
                + "    v timeuuid,\n"
                + "    PRIMARY KEY (k, c1, c2, c3)\n"
                + ") WITH COMPACT STORAGE;", keyspace);

        cql3.put("sparse", sparse);
        cql3.put("static", st);
        cql3.put("counters", counters);
        compact.put("compact_static", compactStatic);
        compact.put("compact_dynamic", compactDynamic);
        compact.put("compact_composite", compactComposite);

        Properties properties = System.getProperties();
        String vmVersion = properties.getProperty("java.vm.specification.version");
        double javaVersion = 1.6;
        if (vmVersion != null) {
            try {
                javaVersion = Double.parseDouble(vmVersion);
            } catch (NumberFormatException e) {
                logger.warn("Could not parse java version from {}.  Assuming {}.", vmVersion, javaVersion);
            }
        }

        // Ordering of compaction options will be dependent on JDK.  See schemaOptionsTest comments for explanation of why this is needed.
        String compactionOptions;
        if (javaVersion >= 1.8)
            compactionOptions = "   AND compaction = { 'sstable_size_in_mb' : 15, 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy' }\n";
        else
            compactionOptions = "   AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb' : 15 }\n";

        withOptions = String.format("CREATE TABLE %s.with_options (\n"
                + "    k text,\n"
                + "    v1 int,\n"
                + "    v2 int,\n"
                + "    i int,\n"
                + "    PRIMARY KEY (k, v1, v2)\n"
                + ") WITH CLUSTERING ORDER BY (v1 DESC, v2 ASC)\n"
                + "   AND read_repair_chance = 0.5\n"
                + "   AND dclocal_read_repair_chance = 0.6\n"
                + "   AND replicate_on_write = true\n"
                + "   AND gc_grace_seconds = 42\n"
                + "   AND bloom_filter_fp_chance = 0.01\n"
                + "   AND caching = 'ALL'\n"
                + "   AND comment = 'My awesome table'\n"
                + compactionOptions
                + "   AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor', 'chunk_length_kb' : 128 };", keyspace);

        List<String> allDefs = new ArrayList<String>();
        allDefs.addAll(cql3.values());
        allDefs.addAll(compact.values());
        allDefs.add(withOptions);
        return allDefs;
    }

    private static String stripOptions(String def, boolean keepFirst) {
        if (keepFirst)
            return def.split("\n   AND ")[0] + ';';
        else
            return def.split(" WITH ")[0] + ';';
    }

    // Note: this test is a bit fragile in the sense that it rely on the exact
    // string formatting of exportAsString, but it's a very simple/convenient
    // way to check we correctly handle schemas so it's probably not so bad.
    // In particular, exportAsString *does not* guarantee that you'll get
    // exactly the same string than the one used to create the table.
    @Test(groups = "short")
    public void schemaExportTest() {

        KeyspaceMetadata metadata = cluster.getMetadata().getKeyspace(keyspace);

        for (Map.Entry<String, String> tableEntry : cql3.entrySet()) {
            String table = tableEntry.getKey();
            String def = tableEntry.getValue();
            assertEquals(stripOptions(metadata.getTable(table).exportAsString(), false), def);
        }

        for (Map.Entry<String, String> tableEntry : compact.entrySet()) {
            String table = tableEntry.getKey();
            String def = tableEntry.getValue();
            assertEquals(stripOptions(metadata.getTable(table).exportAsString(), true), def);
        }
    }

    // Same remark as the preceding test
    @Test(groups = "short")
    public void schemaExportOptionsTest() {
        TableMetadata metadata = cluster.getMetadata().getKeyspace(keyspace).getTable("with_options");

        String withOpts = withOptions;
        VersionNumber version = TestUtils.findHost(cluster, 1).getCassandraVersion();

        if (version.getMajor() == 2) {
            // Strip the last ';'
            withOpts = withOpts.substring(0, withOpts.length() - 1) + '\n';

            // With C* 2.x we'll have a few additional options
            withOpts += "   AND default_time_to_live = 0\n"
                    + "   AND speculative_retry = '99.0PERCENTILE'\n";

            if (version.getMinor() == 0) {
                // With 2.0 we'll have one more options
                withOpts += "   AND index_interval = 128;";
            } else {
                // With 2.1 we have different options, the caching option changes and replicate_on_write disappears
                withOpts += "   AND min_index_interval = 128\n"
                        + "   AND max_index_interval = 2048;";

                withOpts = withOpts.replace("caching = 'ALL'",
                        "caching = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL' }")
                        .replace("   AND replicate_on_write = true\n", "");
            }
        }
        assertEquals(metadata.exportAsString(), withOpts);
    }
}
