package com.gridgain.ignite.ggnode.feeder;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.ssl.SslContextFactory;
import org.gridgain.grid.configuration.GridGainConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ScratchPad {

    public static final String SDEMO_SPRING_CFG_PATH = "C:\\clients\\Schwab\\demo\\gg\\config\\nebula-client-config.xml";

    public static final char[] KEY_STORE_PASSWORD = {'1', '2', '3', '4', '5'};
    public static final char[] TRUST_STORE_PASSWORD = {'1', '2', '3', '4', '5'};

    private static class ScratchClient {

        @QuerySqlField
        private long scratchClientId;

        @QuerySqlField
        private String scratchClientName;

        @QuerySqlField
        private String scratchClientStatus;

        public ScratchClient(long id) {
            this.scratchClientId = id;
            this.scratchClientName = String.format("C%07d", id);
            this.scratchClientStatus = "New";
        }

        public long getId() {
            return scratchClientId;
        }

        public String getName() {
            return scratchClientName;
        }

        public String getStatus() {
            return scratchClientStatus;
        }
    }

    private static class ScratchAccount {

        @QuerySqlField
        private long scratchAccountId;

        @QuerySqlField
        private long scratchAccountClientId;

        @QuerySqlField
        private String scratchAccountName;

        @QuerySqlField
        private long scratchAccountBalance;

        public ScratchAccount(long id, long clientId, long balance) {
            this.scratchAccountId = id;
            this.scratchAccountClientId = clientId;
            this.scratchAccountName = String.format("C%d.A%d", id, clientId);
            this.scratchAccountBalance = balance;
        }

        public long getScratchAccountId() {
            return scratchAccountId;
        }

        public long getScratchAccountClientId() {
            return scratchAccountClientId;
        }

        public String getScratchAccountName() {
            return scratchAccountName;
        }

        public long getScratchAccountBalance() {
            return scratchAccountBalance;
        }
    }

    private static class ScratchAccountKey {
        @QuerySqlField
        private long accountId;

        @QuerySqlField
        @AffinityKeyMapped
        private long clientId;

        public ScratchAccountKey(long accountId, long clientId) {
            this.accountId = accountId;
            this.clientId = clientId;
        }

        public long getAccountId() {
            return accountId;
        }

        public long getClientId() {
            return clientId;
        }
    }

    public static void main(String[] args) {

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePassword(KEY_STORE_PASSWORD);
        sslContextFactory.setTrustStorePassword(TRUST_STORE_PASSWORD);

        // Create client configuration.
        // Replace the {login} and {password} with your cluster credentials.
        System.setProperty("IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED", "true");
        SecurityCredentials clientCredentials = new SecurityCredentials("sdemo", "my1testkey");
        IgniteConfiguration cfg = new IgniteConfiguration()
                .setClientMode(true)
                .setDiscoverySpi(new TcpDiscoverySpi()
                        .setIpFinder(new TcpDiscoveryVmIpFinder()
                                .setAddresses(Collections.singleton(
                                        "5558dc15-f0a7-4593-a1fa-65e0a7dcf853.gridgain-nebula.com:47500"))))
                .setCommunicationSpi(new TcpCommunicationSpi()
                        .setForceClientToServerConnections(true))
                .setPluginConfigurations(new GridGainConfiguration()
                        .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(clientCredentials))
                        .setRollingUpdatesEnabled(true))
                .setSslContextFactory(sslContextFactory);

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration()
                .setPeerClassLoadingEnabled(true)
                .setClientMode(true);

        // try (Ignite ignite = Ignition.start(igniteConfiguration)) {
        try (Ignite ignite = Ignition.start(cfg)) {

                var clientCacheConfiguration = new CacheConfiguration<Long,ScratchClient>()
                        .setName("ScratchClient")
                        .setIndexedTypes(Long.class, ScratchClient.class)
                        .setBackups(1);
                var accountCacheConfiguration = new CacheConfiguration<ScratchAccountKey,ScratchAccount>()
                        .setName("ScratchAccount")
                        .setIndexedTypes(ScratchAccountKey.class, ScratchAccount.class)
                        .setBackups(1);
                var clientCache = ignite.getOrCreateCache(clientCacheConfiguration);
                var accountCache = ignite.getOrCreateCache(accountCacheConfiguration);

                final long MAX_ACCOUNT = 10; // 10_000;
                final long MAX_CLIENT = 80; // 1_000_000_000;

                // Option 1
            /*
            ignite.compute().run(() -> {
                Random rnd = new Random();
                try (var clientDS = ignite.<Long, BinaryObject>dataStreamer("Client")) {
                    clientDS.keepBinary(true);
                    for (var client = 0L; client < MAX_CLIENT; client++) {
                        var v = ignite.binary().builder("Scratchpad$Client");
                        v.setField("name", "Client " + client);
                        clientDS.addData(client, v.build());
                    }
                }
                try (var ds = ignite.<BinaryObject, BinaryObject>dataStreamer("Account")) {
                    ds.keepBinary(true);
                    for (var account = 0L; account < MAX_ACCOUNT; account++) {
                        for (var client = 0L; client < MAX_CLIENT; client++) {
                            var k = ignite.binary().builder("Scratchpad$AccountKey");
                            k.setField("accountId", account);
                            k.setField("clientId", client);
                            var v = ignite.binary().builder("Scratchpad$Account");
                            v.setField("name", "Account " + account);
                            v.setField("balance", rnd.nextLong());
                            ds.addData(k.build(), v.build());
                        }
                    }
                }
            });

             */

                // Option 2
                // Step 1: load clients
                System.out.println(String.format("Populate Clients"));
                IgniteCompute compute = ignite.compute();
                //ignite.compute().run(() -> {
                //    System.out.println("Hello from Grid");
                //});
                for (String word : "Print words on different cluster nodes".split(" ")) {
                    compute.run(() -> System.out.println(word));
                }

                System.out.println(String.format("Scan Query"));
                try (var qryCursor = clientCache.query(new ScanQuery<Long, ScratchClient>()))
                {
                    qryCursor.forEach(
                            entry -> System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()));
                }

                System.out.println(String.format("SQL Query"));
                SqlFieldsQuery sql = new SqlFieldsQuery("SELECT * FROM ScratchClient");

                // Iterate over the result set.
                try (QueryCursor<List<?>> cursor = clientCache.query(sql)) {
                    for (List<?> row : cursor)
                        System.out.println("scratchClientName=" + row.get(1));
                }


            /*
            // Step 2: load accounts
            var f = new LinkedList<IgniteFuture<Void>>();
            for (var p = 0; p < ignite.affinity("Account").partitions(); p++) {
                int finalP = p;
                var res = ignite.compute().affinityRunAsync(Arrays.asList("Account"), p, () -> {
                    Random rnd = new Random();
                    var q = new ScanQuery<Long,BinaryObject>()
                            .setPartition(finalP);
                    for (Cache.Entry<Long,BinaryObject> r :  clientCache.withKeepBinary().query(q)) {
                        var client = r.getKey();
                        for (var account = 0L; account < MAX_ACCOUNT; account++) {
                            var k = ignite.binary().builder("Scratchpad$AccountKey")
                                    .setField("accountId", account)
                                    .setField("clientId", client)
                                    .build();
                            var v = ignite.binary().builder("Scratchpad$Account")
                                    .setField("name", "Account " + account)
                                    .setField("balance", rnd.nextLong())
                                    .build();
                            // put / putAll // data streamer?
                            accountCache.withKeepBinary().put(k, v);
                        }
                    }
                });
                f.add(res);
            }
            f.forEach(IgniteFuture::get);
            */
            //}
        }
    }
}
