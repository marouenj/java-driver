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

import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.*;
import org.assertj.core.api.Fail;
import org.scassandra.http.client.ClosedConnectionConfig;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.Result;
import static org.scassandra.http.client.PrimingRequest.Result.*;
import static org.scassandra.http.client.PrimingRequest.then;

public class FallthroughRetryPolicyIntegrationTest extends AbstractRetryPolicyIntegrationTest {
    public FallthroughRetryPolicyIntegrationTest() {
        super(FallthroughRetryPolicy.INSTANCE);
    }

    @Test(groups = "short")
    public void should_rethrow_on_read_timeout_with_0_receivedResponses() {
        simulateError(1, read_request_timeout);

        try {
            query();
            fail("expected a ReadTimeoutException");
        } catch (ReadTimeoutException e) {/*expected*/ }

        assertOnReadTimeoutWasCalled(1);
        assertThat(errors.getRetriesOnReadTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_on_write_timeout_with_SIMPLE_write_type() {
        simulateError(1, write_request_timeout);

        try {
            query();
            fail("expected a WriteTimeoutException");
        } catch (WriteTimeoutException e) {/*expected*/}

        assertOnWriteTimeoutWasCalled(1);
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_on_unavailable() {
        simulateError(1, unavailable);

        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {/*expected*/}

        assertOnUnavailableWasCalled(1);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_try_next_host_on_client_timeouts() {
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1);
        try {
            scassandras
                    .node(1).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withThen(then().withFixedDelay(1000L).withRows(row("result", "result1")))
                    .build());
            scassandras
                    .node(2).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withThen(then().withFixedDelay(1000L).withRows(row("result", "result2")))
                    .build());
            scassandras
                    .node(3).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withThen(then().withFixedDelay(1000L).withRows(row("result", "result3")))
                    .build());
            try {
                query();
                fail("expected a NoHostAvailableException");
            } catch (NoHostAvailableException e) {
                assertThat(e.getErrors().keySet()).hasSize(3).containsOnly(
                        host1.getSocketAddress(),
                        host2.getSocketAddress(),
                        host3.getSocketAddress());
                assertThat(e.getErrors().values()).extractingResultOf("getMessage").containsOnlyOnce(
                        String.format("[%s]: Timed out waiting for server response", host1.getSocketAddress()),
                        String.format("[%s]: Timed out waiting for server response", host2.getSocketAddress()),
                        String.format("[%s]: Timed out waiting for server response", host3.getSocketAddress())
                );
            }
            assertOnRequestErrorWasCalled(3);
            assertThat(errors.getRetries().getCount()).isEqualTo(3);
            assertThat(errors.getClientTimeouts().getCount()).isEqualTo(3);
            assertThat(errors.getRetriesOnClientTimeout().getCount()).isEqualTo(3);
            assertQueried(1, 1);
            assertQueried(2, 1);
            assertQueried(3, 1);
        } finally {
            cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS);
        }
    }


    @Test(groups = "short", dataProvider = "serverSideErrors")
    public void should_try_next_host_on_server_side_error(Result error, Class<? extends DriverException> exception) {
        simulateError(1, error);
        simulateError(2, error);
        simulateError(3, error);
        try {
            query();
            Fail.fail("expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertThat(e.getErrors().keySet()).hasSize(3).containsOnly(
                    host1.getSocketAddress(),
                    host2.getSocketAddress(),
                    host3.getSocketAddress());
            assertThat(e.getErrors().values()).hasOnlyElementsOfType(exception);
        }
        assertOnRequestErrorWasCalled(3);
        assertThat(errors.getOthers().getCount()).isEqualTo(3);
        assertThat(errors.getRetries().getCount()).isEqualTo(3);
        assertThat(errors.getRetriesOnOtherErrors().getCount()).isEqualTo(3);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 1);
    }


    @Test(groups = "short", dataProvider = "connectionErrors")
    public void should_try_next_host_on_connection_error(ClosedConnectionConfig.CloseType closeType) {
        simulateError(1, PrimingRequest.Result.closed_connection, new ClosedConnectionConfig(closeType));
        simulateError(2, PrimingRequest.Result.closed_connection, new ClosedConnectionConfig(closeType));
        simulateError(3, PrimingRequest.Result.closed_connection, new ClosedConnectionConfig(closeType));
        try {
            query();
            Fail.fail("expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertThat(e.getErrors().keySet()).hasSize(3).containsOnly(
                    host1.getSocketAddress(),
                    host2.getSocketAddress(),
                    host3.getSocketAddress());
        }
        assertOnRequestErrorWasCalled(3);
        assertThat(errors.getRetries().getCount()).isEqualTo(3);
        assertThat(errors.getConnectionErrors().getCount()).isEqualTo(3);
        assertThat(errors.getIgnoresOnConnectionError().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnConnectionError().getCount()).isEqualTo(3);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 1);
    }
}
