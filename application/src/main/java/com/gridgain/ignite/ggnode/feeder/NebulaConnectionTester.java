package com.gridgain.ignite.ggnode.feeder;

import org.apache.ignite.*;
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

public class NebulaConnectionTester {

    public static final int SDEMO_NUM_CLIENTS = 1000000;              // 1M clients (default)
    public static final int SDEMO_NUM_ACCOUNTS_PER_CLIENT = 10000;    // 10K accounts per client (default)
    // public static final String SDEMO_SPRING_CFG_PATH = "client-config.xml";
    // public static final String SDEMO_SPRING_CFG_PATH = "C:\\clients\\Schwab\\demo\\gg\\config\\nebula-client-config.xml";

    public static final char[] KEY_STORE_PASSWORD = {'1', '2', '3', '4', '5'};
    public static final char[] TRUST_STORE_PASSWORD = {'1', '2', '3', '4', '5'};

    public static void main(String[] args) throws NumberFormatException {

        // Set any specified runtime arguments: numClients, numAccountsPerClient, and sprigCfgPath
        //int numClients = args.length > 0 ? Integer.parseInt(args[0]) : SDEMO_NUM_CLIENTS;
        //int numAccountsPerClient = args.length > 1 ? Integer.parseInt(args[1]) : SDEMO_NUM_ACCOUNTS_PER_CLIENT;
        //String springCfgPath = (args.length <= 2 || args[2] == null || args[2].isBlank()) ? SDEMO_SPRING_CFG_PATH : args[2].trim();

        //System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");
        //Ignition.setClientMode(true);

        //try (Ignite start = Ignition.start(springCfgPath)) {
        //    // ClientFeeder.loadSDemoClients(numClients);
        //    ClientAndAccountsFeeder.loadSDemoAccounts(numClients, numAccountsPerClient);
        //}

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

        // Connect to the cluster.
        try (Ignite client = Ignition.start(cfg)) {
            // Use the API.
            IgniteCache<Integer, String> cache =  client.getOrCreateCache(new CacheConfiguration<Integer, String>()
                    .setName("Test")
                    .setBackups(1)
            );
            cache.put(1, "foo");
            System.out.println(">>>    " + cache.get(1));

            System.out.println(">>>  Test Compute");
            IgniteCompute compute = client.compute();
            for (String word : "Print words on different cluster nodes".split(" ")) {
                compute.run(() -> System.out.println(word));
            }

            System.out.println(">>>   End Test");
        }
    }
}
