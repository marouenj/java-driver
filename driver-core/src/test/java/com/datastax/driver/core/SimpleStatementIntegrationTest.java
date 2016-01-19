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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleStatementIntegrationTest extends CCMTestsSupport {
    @Override
    public Collection<String> createTestFixtures() {
        return Lists.newArrayList(
                "CREATE TABLE users(id int PRIMARY KEY, name text)",
                "INSERT INTO users(id, name) VALUES (1, 'test')"
        );
    }

    @Test(groups = "short")
    public void should_execute_query_with_named_value() {
        // Given
        SimpleStatement statement = new SimpleStatement("SELECT * FROM users WHERE id = :id",
                ImmutableMap.<String, Object>of("id", 1));

        // When
        Row row = session.execute(statement).one();

        // Then
        assertThat(row).isNotNull();
        assertThat(row.getString("name")).isEqualTo("test");
    }
}
