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
package com.datastax.driver.auth;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.DseVersion;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DseVersion
public class DseAuthProviderTest {

    // Realm for the KDC.
    private static final String realm = "DATASTAX.COM";
    private static final String address = CCMBridge.IP_PREFIX + "1";
    // Principal for the default cassandra user.
    private static final String userPrincipal = "cassandra@" + realm;
    // Principal for a user that doesn't exist.
    private static final String unknownPrincipal = "unknown@" + realm;
    private File userKeytab;
    private File unknownKeytab;

    EmbeddedADS adsServer = EmbeddedADS.builder()
            .withKerberos()
            .withRealm(realm)
            .withAddress(address).build();

    CCMBridge ccm;

    @BeforeClass(groups = "long")
    public void setup() throws Exception {
        // Start ldap/kdc server.
        adsServer.start();

        // Create users and keytabs for the DSE principal and cassandra user.
        File dseKeytab = adsServer.addUserAndCreateKeytab("dse", "dse", "dse/" + address + "@" + realm);
        userKeytab = adsServer.addUserAndCreateKeytab("cassandra", "cassandra", userPrincipal);
        unknownKeytab = adsServer.createKeytab("unknown", "unknown", unknownPrincipal);

        // Configure cluster to use kerberos.
        ccm = CCMBridge.builder("test")
                .withCassandraConfiguration("authenticator", "com.datastax.bdp.cassandra.auth.KerberosAuthenticator")
                .withDSEConfiguration("kerberos_options.keytab", dseKeytab.getAbsolutePath())
                .withDSEConfiguration("kerberos_options.service_principal", "dse/_HOST@" + realm)
                .withDSEConfiguration("kerberos_options.qop", "auth")
                .notStarted()
                .build();

        // Start and provide path to the specialized krb5.conf for the embedded server. This is required for DSE to
        // properly login with sasl.
        ccm.start(1, "-Dcassandra.superuser_setup_delay_ms=0",
                "-Djava.security.krb5.conf=" + adsServer.getKrb5Conf().getAbsolutePath());
    }

    @AfterClass(groups = "long")
    public void teardown() throws Exception {
        adsServer.stop();
        if (ccm != null) {
            ccm.remove();
        }
    }

    /**
     * Ensures that a Cluster can be established to a DSE server secured with Kerberos and that simple queries can
     * be made using a client configuration that provides a keytab file.
     *
     * @test_category authentication
     */
    @Test(groups = "long")
    public void should_authenticate_using_kerberos_with_keytab() throws Exception {
        connectAndQuery(keytabClient(userKeytab, userPrincipal));
    }

    /**
     * Ensures that a Cluster can be established to a DSE server secured with Kerberos and that simple queries can
     * be made using a client configuration that uses the ticket cache.  This test will only run on unix platforms
     * since it uses kinit to acquire tickets and kdestroy to destroy them.
     *
     * @test_category authentication
     */
    @Test(groups = "long")
    public void should_authenticate_using_kerberos_with_ticket() throws Exception {
        String osName = System.getProperty("os.name", "").toLowerCase();
        boolean isUnix = osName.contains("mac") || osName.contains("darwin") || osName.contains("nux");
        if (!isUnix) {
            throw new SkipException("This test requires a unix-based platform with kinit & kdestroy installed.");
        }
        acquireTicket(userPrincipal, userKeytab);
        try {
            connectAndQuery(ticketClient(userPrincipal));
        } finally {
            destroyTicket(userPrincipal);
        }
    }

    /**
     * Validates that a {@link NoHostAvailableException} is thrown when using a ticket-based configuration and no
     * such ticket exists in the user's cache.  This is expected because we shouldn't be able to establish connection
     * to a cassandra node if we cannot authenticate.
     *
     * @test_category authentication
     */
    @Test(groups = "long", expectedExceptions = NoHostAvailableException.class)
    public void should_not_authenticate_if_no_ticket_in_cache() throws Exception {
        connectAndQuery(ticketClient(userPrincipal));
    }

    /**
     * Validates that a {@link NoHostAvailableException} is thrown when using a keytab-based configuration and no
     * such user exists for the given principal.  This is expected because we shouldn't be able to establish connection
     * to a cassandra node if we cannot authenticate.
     *
     * @test_category authentication
     */
    @Test(groups = "long", expectedExceptions = NoHostAvailableException.class)
    public void should_not_authenticate_if_keytab_does_not_map_to_valid_principal() throws Exception {
        connectAndQuery(keytabClient(unknownKeytab, unknownPrincipal));
    }

    /**
     * Connects using {@link DseAuthProvider} and the given config file for jaas.
     */
    private void connectAndQuery(Configuration configuration) {
        Cluster cluster = Cluster.builder()
                .addContactPoint(CCMBridge.IP_PREFIX + "1")
                .withAuthProvider(new DseAuthProvider(configuration)).build();

        try {
            Session session = cluster.connect();
            Row row = session.execute("select * from system.local").one();
            assertThat(row).isNotNull();
        } finally {
            cluster.close();
        }
    }

    /**
     * Executes the given command with KRB5_CONFIG environment variable pointing to the specialized config file
     * for the embedded KDC server.
     */
    private void executeCommand(String command) throws IOException {
        Map<String, String> environmentMap = ImmutableMap.<String, String>builder()
                .put("KRB5_CONFIG", adsServer.getKrb5Conf().getAbsolutePath()).build();
        CommandLine cli = CommandLine.parse(command);
        Executor executor = new DefaultExecutor();
        int retValue = executor.execute(cli, environmentMap);
        assertThat(retValue).isZero();
    }

    /**
     * Acquires a ticket into the cache with the tgt using kinit command with the given principal and keytab file.
     */
    private void acquireTicket(String principal, File keytab) throws IOException {
        executeCommand(String.format("kinit -t %s -k %s", keytab.getAbsolutePath(), principal));
    }

    /**
     * Destroys all tickets in the cache with given principal.
     */
    private void destroyTicket(String principal) throws IOException {
        executeCommand(String.format("kdestroy -p %s", principal));
    }

    /**
     * Creates a configuration that depends on the ticket cache for authenticating the given user.
     */
    private Configuration ticketClient(final String principal) {
        return new Configuration() {

            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                Map<String, String> options = ImmutableMap.<String, String>builder()
                        .put("principal", principal)
                        .put("useTicketCache", "true")
                        .put("renewTGT", "true").build();

                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
            }
        };
    }

    /**
     * Creates a configuration that depends on the given keytab file for authenticating the given user.
     */
    private Configuration keytabClient(final File keytabFile, final String principal) {
        return new Configuration() {

            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                Map<String, String> options = ImmutableMap.<String, String>builder()
                        .put("principal", principal)
                        .put("useKeyTab", "true")
                        .put("keyTab", keytabFile.getAbsolutePath()).build();

                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
            }
        };
    }
}
