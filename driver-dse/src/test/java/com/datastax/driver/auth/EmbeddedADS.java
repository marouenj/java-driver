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

import com.datastax.driver.core.TestUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.directory.api.ldap.model.constants.SchemaConstants;
import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.api.ldap.model.csn.CsnFactory;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.api.ldap.schema.manager.impl.DefaultSchemaManager;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.core.DefaultDirectoryService;
import org.apache.directory.server.core.api.CacheService;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.DnFactory;
import org.apache.directory.server.core.api.InstanceLayout;
import org.apache.directory.server.core.api.schema.SchemaPartition;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmPartition;
import org.apache.directory.server.core.partition.ldif.LdifPartition;
import org.apache.directory.server.core.shared.DefaultDnFactory;
import org.apache.directory.server.kerberos.KerberosConfig;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.sasl.MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.cramMD5.CramMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.digestMD5.DigestMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.plain.PlainMechanismHandler;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.UdpTransport;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * A convenience utility for running an Embedded Apache Directory Service with LDAP and optionally a Kerberos
 * Key Distribution Server.  By default listens for LDAP on 10389 and Kerberos on 60088.  You can use something like
 * <a href="https://directory.apache.org/studio/">Apache Directory Studio</a> to verify the server is configured and
 * running correctly by connecting to localhost:10389 with username 'uid=admin,ou=system' and password 'secret'.
 * <p>
 * <b>Note:</b> This should only be used for development and testing purposes.
 */
public class EmbeddedADS {

    private final String dn;

    private final String realm;

    private int kdcPort;

    private int ldapPort;

    private final boolean kerberos;

    private final String address;

    private volatile boolean isInit = false;

    private DirectoryService service;

    private LdapServer ldapServer;

    private KdcServer kdcServer;

    private Dn usersDN;

    private File krb5Conf;

    private EmbeddedADS(String dn, String realm, String address, int ldapPort, boolean kerberos, int kdcPort) {
        this.dn = dn;
        this.realm = realm;
        this.address = address;
        this.ldapPort = ldapPort;
        this.kerberos = kerberos;
        this.kdcPort = kdcPort;
    }

    public void start() throws Exception {
        if (isInit) {
            return;
        }
        isInit = true;
        File workDir = Files.createTempDir();

        // Set system properties required for kerberos auth to work.  Unfortunately admin_server cannot be
        // expressed via System properties (like realm and kdc can), thus we must unfortunately create a config file.
        if (kerberos) {
            kdcPort = kdcPort != -1 ? kdcPort : TestUtils.findAvailablePort(60088);
            krb5Conf = File.createTempFile("krb5", ".conf");
            String config = String.format("[libdefaults]%n" +
                    "default_realm = %s%n%n" +
                    "[realms]%n" +
                    "%s = {%n" +
                    "  kdc = %s:%d%n" +
                    "  admin_server = %s:%d%n" +
                    "}%n", realm, realm, address, kdcPort, address, kdcPort);
            FileOutputStream fios = new FileOutputStream(krb5Conf);
            try {
                PrintWriter pw = new PrintWriter(fios);
                pw.write(config);
                pw.close();
            } finally {
                fios.close();
            }
            System.setProperty("java.security.krb5.conf", krb5Conf.getAbsolutePath());
            // Useful options for debugging.
            // System.setProperty("sun.security.krb5.debug", "true");
            // System.setProperty("java.security.debug", "configfile,configparser,gssloginconfig");
        }

        // Initialize sevice and set its filesystem layout.
        service = new DefaultDirectoryService();
        InstanceLayout layout = new InstanceLayout(workDir);
        service.setInstanceLayout(layout);

        // Disable ChangeLog as we don't need change tracking.
        service.getChangeLog().setEnabled(false);
        // Denormalizes attribute DNs to be human readable, i.e uid=admin,ou=system instead of 0.9.2.3=admin,2.5=system)
        service.setDenormalizeOpAttrsEnabled(true);

        // Create and init cache service which will be used for caching DNs, among other things.
        CacheService cacheService = new CacheService();
        cacheService.initialize(layout);

        // Create and load SchemaManager which will create the default schema partition.
        SchemaManager schemaManager = new DefaultSchemaManager();
        service.setSchemaManager(schemaManager);
        schemaManager.loadAllEnabled();

        // Create SchemaPartition from schema manager and load ldif from schema directory.
        SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
        LdifPartition ldifPartition = new LdifPartition(schemaManager, service.getDnFactory());
        ldifPartition.setPartitionPath(new File(layout.getPartitionsDirectory(), "schema").toURI());
        schemaPartition.setWrappedPartition(ldifPartition);
        service.setSchemaPartition(schemaPartition);

        // Create a DN factory which can be used to create and cache DNs.
        DnFactory dnFactory = new DefaultDnFactory(schemaManager, cacheService.getCache("dnCache"));
        service.setDnFactory(dnFactory);

        // Create mandatory system partition.  This is used for storing server configuration.
        JdbmPartition systemPartition = createPartition("system", dnFactory.create(ServerDNConstants.SYSTEM_DN));
        service.setSystemPartition(systemPartition);

        // Now that we have a schema and system partition, start up the directory service.
        service.startup();

        // Create partition where user, tgt and ldap principals will live.
        Dn partitionDn = dnFactory.create(dn);
        String dnName = partitionDn.getRdn().getValue().getString();
        JdbmPartition partition = createPartition(dnName, partitionDn);

        // Add a context entry so the partition can be referenced by entries.
        Entry context = service.newEntry(partitionDn);
        context.add("objectClass", "top", "domain", "extensibleObject");
        context.add(partitionDn.getRdn().getType(), dnName);
        partition.setContextEntry(context);
        service.addPartition(partition);

        // Create users domain.
        usersDN = partitionDn.add(dnFactory.create("ou=users"));
        Entry usersEntry = service.newEntry(usersDN);
        usersEntry.add("objectClass", "organizationalUnit", "top");
        usersEntry.add("ou", "users");
        if (kerberos) {
            usersEntry = kerberize(usersEntry);
        }
        service.getAdminSession().add(usersEntry);

        // Uncomment to allow to connect to ldap server without credentials for convenience.
        // service.setAllowAnonymousAccess(true);

        startLdap();

        // Create sasl and krbtgt principals and start KDC if kerberos is enabled.
        if (kerberos) {
            // Ticket Granting Ticket entry.
            Dn tgtDN = usersDN.add(dnFactory.create("uid=krbtgt"));
            String servicePrincipal = "krbtgt/" + realm + "@" + realm;
            Entry tgtEntry = service.newEntry(tgtDN);
            tgtEntry.add("objectClass", "person", "inetOrgPerson", "top", "krb5KDCEntry", "uidObject", "krb5Principal");
            tgtEntry.add("krb5KeyVersionNumber", "0");
            tgtEntry.add("krb5PrincipalName", servicePrincipal);
            tgtEntry.add("uid", "krbtgt");
            tgtEntry.add("userPassword", "secret");
            tgtEntry.add("sn", "Service");
            tgtEntry.add("cn", "KDC Service");
            service.getAdminSession().add(kerberize(tgtEntry));

            // LDAP SASL principal.
            String saslPrincipal = "ldap/" + address + "@" + realm;
            ldapServer.setSaslPrincipal(saslPrincipal);
            Dn ldapDN = usersDN.add(dnFactory.create("uid=ldap"));
            Entry ldapEntry = service.newEntry(ldapDN);
            ldapEntry.add("objectClass", "top", "person", "inetOrgPerson", "krb5KDCEntry", "uidObject", "krb5Principal");
            ldapEntry.add("krb5KeyVersionNumber", "0");
            ldapEntry.add("krb5PrincipalName", saslPrincipal);
            ldapEntry.add("uid", "ldap");
            ldapEntry.add("userPassword", "secret");
            ldapEntry.add("sn", "Service");
            ldapEntry.add("cn", "LDAP Service");
            service.getAdminSession().add(kerberize(ldapEntry));

            startKDC(servicePrincipal);
        }
    }

    /**
     * @return A specialized krb5.conf file that defines and defaults to the domain expressed by this server.
     */
    public File getKrb5Conf() {
        return krb5Conf;
    }

    /**
     * Adds a user with the given password and principal name and creates a keytab file for authenticating
     * with that user's principal.
     *
     * @param user      Username to login with (i.e. cassandra).
     * @param password  Password to authenticate with.
     * @param principal Principal representing the server (i.e. cassandra@DATASTAX.COM).
     * @return Generated keytab file for this user.
     */
    public File addUserAndCreateKeytab(String user, String password, String principal) throws IOException, LdapException {
        addUser(user, password, principal);
        return createKeytab(user, password, principal);
    }

    /**
     * Creates a keytab file for authenticating with a given principal.
     *
     * @param user      Username to login with (i.e. cassandra).
     * @param password  Password to authenticate with.
     * @param principal Principal representing the server (i.e. cassandra@DATASTAX.COM).
     * @return Generated keytab file for this user.
     */
    public File createKeytab(String user, String password, String principal) throws IOException {
        File keytabFile = File.createTempFile(user, ".keytab");
        Keytab keytab = Keytab.getInstance();

        KerberosTime timeStamp = new KerberosTime(System.currentTimeMillis());

        Map<EncryptionType, EncryptionKey> keys = KerberosKeyFactory
                .getKerberosKeys(principal, password);

        KeytabEntry keytabEntry = new KeytabEntry(
                principal,
                0,
                timeStamp,
                (byte) 0,
                keys.get(EncryptionType.AES256_CTS_HMAC_SHA1_96));

        keytab.setEntries(Collections.singletonList(keytabEntry));
        keytab.write(keytabFile);
        return keytabFile;
    }

    /**
     * Adds a user with the given password, does not create necessary kerberos attributes.
     *
     * @param user     Username to login with (i.e. cassandra).
     * @param password Password to authenticate with.
     */
    public void addUser(String user, String password) throws LdapException {
        addUser(user, password, null);
    }

    /**
     * Adds a user with the given password and principal.  If principal is specified and kerberos is enabled, user is
     * created with the necessary attributes to authenticate with kerberos (entryCsn, entryUuid, etc.).
     *
     * @param user      Username to login with (i.e. cassandra).
     * @param password  Password to authenticate with.
     * @param principal Principal representing the server (i.e. cassandra@DATASTAX.COM).
     */
    public void addUser(String user, String password, String principal) throws LdapException {
        Preconditions.checkState(isInit);
        Dn userDN = usersDN.add("uid=" + user);
        Entry userEntry = service.newEntry(userDN);
        if (kerberos && principal != null) {
            userEntry.add("objectClass", "organizationalPerson", "person", "extensibleObject", "inetOrgPerson", "top", "krb5KDCEntry", "uidObject", "krb5Principal");
            userEntry.add("krb5KeyVersionNumber", "0");
            userEntry.add("krb5PrincipalName", principal);
            userEntry = kerberize(userEntry);
        } else {
            userEntry.add("objectClass", "organizationalPerson", "person", "extensibleObject", "inetOrgPerson", "top", "uidObject");
        }
        userEntry.add("uid", user);
        userEntry.add("sn", user);
        userEntry.add("cn", user);
        userEntry.add("userPassword", password);
        service.getAdminSession().add(userEntry);
    }

    /**
     * Stops the server(s) if running.
     */
    public void stop() {
        if (ldapServer != null)
            ldapServer.stop();
        if (kdcServer != null)
            kdcServer.stop();
    }

    /**
     * Adds attributes to the given Entry which will enable krb5key attributes to be added to them.
     *
     * @param entry Entry to add attributes to.
     * @return The provided entry.
     */
    private Entry kerberize(Entry entry) throws LdapException {
        // Add csn and uuids for kerberos, this is needed to generate krb5keys.
        entry.add(SchemaConstants.ENTRY_CSN_AT, new CsnFactory(0).newInstance().toString());
        entry.add(SchemaConstants.ENTRY_UUID_AT, UUID.randomUUID().toString());
        return entry;
    }

    /**
     * Creates a {@link JdbmPartition} with the given id and DN.
     *
     * @param id Id to create partition with.
     * @param dn Distinguished Name to use to create partition.
     * @return Created partition.
     */
    private JdbmPartition createPartition(String id, Dn dn) throws LdapInvalidDnException {
        JdbmPartition partition = new JdbmPartition(service.getSchemaManager(), service.getDnFactory());
        partition.setId(id);
        partition.setPartitionPath(new File(service.getInstanceLayout().getPartitionsDirectory(), id).toURI());
        partition.setSuffixDn(dn);
        partition.setSchemaManager(service.getSchemaManager());
        return partition;
    }

    /**
     * Starts the LDAP Server with SASL enabled.
     */
    private void startLdap() throws Exception {
        // Create and start LDAP server.
        ldapServer = new LdapServer();

        // Enable SASL layer, this is useful with or without kerberos.
        Map<String, MechanismHandler> mechanismHandlerMap = Maps.newHashMap();
        mechanismHandlerMap.put(SupportedSaslMechanisms.PLAIN, new PlainMechanismHandler());
        mechanismHandlerMap.put(SupportedSaslMechanisms.CRAM_MD5, new CramMd5MechanismHandler());
        mechanismHandlerMap.put(SupportedSaslMechanisms.DIGEST_MD5, new DigestMd5MechanismHandler());
        // GSSAPI is required for kerberos.
        mechanismHandlerMap.put(SupportedSaslMechanisms.GSSAPI, new GssapiMechanismHandler());
        ldapServer.setSaslMechanismHandlers(mechanismHandlerMap);
        ldapServer.setSaslHost(address);
        // Realms only used by DIGEST_MD5 and GSSAPI.
        ldapServer.setSaslRealms(Collections.singletonList(realm));
        ldapServer.setSearchBaseDn(dn);

        ldapPort = ldapPort != -1 ? ldapPort : TestUtils.findAvailablePort(10389);
        ldapServer.setTransports(new TcpTransport(ldapPort));
        ldapServer.setDirectoryService(service);
        if (kerberos) {
            // Add an interceptor to attach krb5keys to created principals.
            KeyDerivationInterceptor interceptor = new KeyDerivationInterceptor();
            interceptor.init(service);
            service.addLast(interceptor);
        }
        ldapServer.start();
    }

    /**
     * Starts the Kerberos Key Distribution Server supporting AES256 using the given principal for the Ticket-granting
     * ticket.
     *
     * @param servicePrincipal TGT principcal service.
     */
    private void startKDC(String servicePrincipal) throws Exception {
        KerberosConfig config = new KerberosConfig();
        // We choose AES256_CTS_HMAC_SHA1_96 for our generated keytabs (this requires JCE).
        config.setEncryptionTypes(Sets.newHashSet(EncryptionType.AES256_CTS_HMAC_SHA1_96));
        config.setSearchBaseDn(dn);
        config.setServicePrincipal(servicePrincipal);

        kdcServer = new KdcServer(config);
        kdcServer.setDirectoryService(service);

        kdcServer.setTransports(new TcpTransport(kdcPort), new UdpTransport(kdcPort));
        kdcServer.start();
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String dn = "dc=datastax,dc=com";

        private String realm = "DATASTAX.COM";

        private boolean kerberos = false;

        private int kdcPort = -1;

        private int ldapPort = -1;

        private String address = "127.0.0.1";

        private Builder() {
        }

        public EmbeddedADS build() {
            return new EmbeddedADS(dn, realm, address, ldapPort, kerberos, kdcPort);
        }

        /**
         * Configures the base DN to create users under.  Defaults to <code>dc=datastax,dc=com</code>.
         */
        public Builder withBaseDn(String dn) {
            this.dn = dn;
            return this;
        }

        /**
         * Configures the realm to use for SASL and Kerberos.  Defaults to <code>DATASTAX.COM</code>.
         */
        public Builder withRealm(String realm) {
            this.realm = realm;
            return this;
        }

        /**
         * Configures the port to use for LDAP.  Defaults to the first available port from 10389+.  Must be
         * greater than 1024.
         */
        public Builder withLdapPort(int port) {
            Preconditions.checkArgument(port > 1024);
            this.ldapPort = port;
            return this;
        }


        /**
         * Configures the port to use for Kerberos KDC.  Defaults to the first available port for 60088+.  Must be
         * greater than 1024.
         */
        public Builder withKerberos(int port) {
            Preconditions.checkArgument(port > 1024);
            this.kdcPort = port;
            return withKerberos();
        }

        /**
         * Configures the server to run with a Kerberos KDC using the first available port for 60088+.
         */
        public Builder withKerberos() {
            this.kerberos = true;
            return this;
        }

        /**
         * Configures the server to be configured to listen with the given address.  Defaults to 127.0.0.1.  You
         * shouldn't need to change this.
         */
        public Builder withAddress(String address) {
            this.address = address;
            return this;
        }
    }
}
