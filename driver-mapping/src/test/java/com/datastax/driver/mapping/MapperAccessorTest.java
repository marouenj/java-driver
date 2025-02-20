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
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

public class MapperAccessorTest extends CCMTestsSupport {

    @Override
    public Collection<String> createTestFixtures() {
        return Lists.newArrayList("CREATE TABLE foo (k int primary key, v text)");
    }

    @Test(groups = "short")
    public void should_implement_toString() {
        SystemAccessor accessor = new MappingManager(session)
                .createAccessor(SystemAccessor.class);

        assertThat(accessor.toString())
                .isEqualTo("SystemAccessor implementation generated by the Cassandra driver mapper");
    }

    @Test(groups = "short")
    public void should_implement_equals_and_hashCode() {
        SystemAccessor accessor = new MappingManager(session)
                .createAccessor(SystemAccessor.class);

        assertThat(accessor).isNotEqualTo(new Object());
        assertThat(accessor.hashCode()).isEqualTo(System.identityHashCode(accessor));
    }

    @Test(groups = "short")
    @CassandraVersion(major = 2.0)
    public void should_allow_null_argument_in_accessor_when_set_by_name() {
        FooAccessor accessor = new MappingManager(session)
                .createAccessor(FooAccessor.class);

        accessor.insert(1, null);
        Row row = session.execute("select * from foo where k = 1").one();

        assertThat(row.getString("v")).isNull();
    }

    @Test(groups = "short")
    public void should_allow_null_argument_in_accessor_when_set_by_index() {
        FooBindMarkerAccessor accessor = new MappingManager(session)
                .createAccessor(FooBindMarkerAccessor.class);

        accessor.insert(1, null);

        Row row = session.execute("select * from foo where k = 1").one();

        assertThat(row.getString("v")).isNull();
    }

    @Test(groups = "short")
    public void should_allow_void_return_type_in_accessor() {
        VoidAccessor accessor = new MappingManager(session).createAccessor(VoidAccessor.class);
        accessor.insert(1, "bar");
        Row row = session.execute("select * from foo where k = 1").one();
        assertThat(row.getInt("k")).isEqualTo(1);
        assertThat(row.getString("v")).isEqualTo("bar");
    }

    @SuppressWarnings("unused")
    @Accessor
    public interface SystemAccessor {
        @Query("select release_version from system.local")
        ResultSet getCassandraVersion();
    }

    @Accessor
    public interface FooAccessor {
        @Query("insert into foo (k, v) values (:k, :v)")
        ResultSet insert(@Param("k") int k, @Param("v") String v);
    }

    @Accessor
    public interface FooBindMarkerAccessor {
        @Query("insert into foo (k, v) values (?, ?)")
        ResultSet insert(int k, String v);
    }

    @Accessor
    public interface VoidAccessor {
        @Query("insert into foo (k, v) values (?, ?)")
        void insert(int k, String v);
    }

}
