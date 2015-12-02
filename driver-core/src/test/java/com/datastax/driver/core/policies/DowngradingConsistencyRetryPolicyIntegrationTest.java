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
package com.datastax.driver.core.policies;

import com.google.common.base.Objects;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import com.datastax.driver.core.*;

import static com.datastax.driver.core.ConsistencyLevel.*;

/**
 * Note: we can't extend {@link AbstractRetryPolicyIntegrationTest} here, because SCassandra doesn't allow custom values for
 * receivedResponses in primed responses.
 * If that becomes possible in the future, we could refactor this test.
 */
public class DowngradingConsistencyRetryPolicyIntegrationTest {

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";
    private static final String CREATE_TABLE = "CREATE TABLE test.foo(k int primary key)";
    private static final String READ_QUERY = "SELECT * FROM test.foo WHERE k = 0";
    private static final String WRITE_QUERY = "INSERT INTO test.foo(k) VALUES (0)";

    @Test(groups = "long")
    public void should_downgrade_if_not_enough_replicas_for_requested_CL() {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            // 3-node cluster, keyspace with RF = 3
            ccm = CCMBridge.builder(this.getClass().getName()).withNodes(3).build();
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withRetryPolicy(Mockito.spy(DowngradingConsistencyRetryPolicy.INSTANCE))
                .build();
            Session session = cluster.connect();

            session.execute(CREATE_KEYSPACE);
            session.execute(CREATE_TABLE);
            session.execute(WRITE_QUERY);

            // All replicas up: should achieve all levels without downgrading
            checkAchievedConsistency(READ_QUERY, ALL, ALL, session);
            checkAchievedConsistency(READ_QUERY, QUORUM, QUORUM, session);
            checkAchievedConsistency(READ_QUERY, ONE, ONE, session);

            ccm.stop(1);
            ccm.waitForDown(1);
            // Two replicas remaining: should downgrade to 2 when CL > 2
            checkAchievedConsistency(READ_QUERY, ALL, TWO, session);
            checkAchievedConsistency(READ_QUERY, QUORUM, QUORUM, session); // since RF = 3, quorum is still achievable with two nodes
            checkAchievedConsistency(READ_QUERY, TWO, TWO, session);
            checkAchievedConsistency(READ_QUERY, ONE, ONE, session);

            ccm.stop(2);
            ccm.waitForDown(2);
            // One replica remaining: should downgrade to 1 when CL > 1
            checkAchievedConsistency(READ_QUERY, ALL, ONE, session);
            checkAchievedConsistency(READ_QUERY, QUORUM, ONE, session);
            checkAchievedConsistency(READ_QUERY, TWO, ONE, session);
            checkAchievedConsistency(READ_QUERY, ONE, ONE, session);

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    @Test(groups = "long")
    public void should_downgrade_EACH_QUORUM_to_ONE() {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            // 2 DC cluster, keyspace with RF = 3
            ccm = CCMBridge.builder(this.getClass().getName()).withNodes(3, 3).build();
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").withUsedHostsPerRemoteDc(1).build())
                .withRetryPolicy(Mockito.spy(DowngradingConsistencyRetryPolicy.INSTANCE))
                .build();
            Session session = cluster.connect();

            session.execute(CREATE_KEYSPACE);
            session.execute(CREATE_TABLE);

            // shut down DC 1 - cannot achieve EACH_QUORUM
            ccm.stop(1);
            ccm.stop(2);
            ccm.stop(3);
            ccm.waitForDown(1);
            ccm.waitForDown(2);
            ccm.waitForDown(3);

            // EACH_QUORUM is only supported for writes
            checkAchievedConsistency(WRITE_QUERY, EACH_QUORUM, ONE, session);

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    private void checkAchievedConsistency(String query, ConsistencyLevel requested, ConsistencyLevel expected, Session session) {
        RetryPolicy retryPolicy = session.getCluster().getConfiguration().getPolicies().getRetryPolicy();
        Mockito.reset(retryPolicy);

        Statement s = new SimpleStatement(query)
            .setConsistencyLevel(requested);

        ResultSet rs = session.execute(s);

        ConsistencyLevel achieved = rs.getExecutionInfo().getAchievedConsistencyLevel();
        // ExecutionInfo returns null when the requested level was met.
        ConsistencyLevel actual = Objects.firstNonNull(achieved, requested);

        assertThat(actual).isEqualTo(expected);

        // If the level was downgraded the policy should have been invoked
        VerificationMode expectedCallsToPolicy = (expected == requested) ? never() : times(1);
        Mockito.verify(retryPolicy, expectedCallsToPolicy).onUnavailable(
            any(Statement.class), any(ConsistencyLevel.class), anyInt(), anyInt(), anyInt());
    }
}
