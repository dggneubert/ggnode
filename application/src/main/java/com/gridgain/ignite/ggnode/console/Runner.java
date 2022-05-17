package com.gridgain.ignite.ggnode.console;

import com.gridgain.ignite.ggnode.cgrid.SumBalancesForAllClientsTask;
import com.gridgain.ignite.ggnode.cgrid.SumBalancesForAllClientsTask2;
import java.math.BigDecimal;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.ssl.SslContextFactory;
import org.gridgain.grid.configuration.GridGainConfiguration;

public class Runner {

    public static void main(String[] args) {
        connect();
        System.out.println("Done!");

    }

    private static void connect() {
        System.setProperty("IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED", "true");
        System.setProperty("IGNITE_QUIET", "false");
        SecurityCredentials clientCredentials =
            new SecurityCredentials("sdemo", "my1testkey");
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
            .setSslContextFactory(new SslContextFactory());

        // Connect to the cluster.
        Ignite client = Ignition.start(cfg);
            // Use the API.
            IgniteCache<Integer, String> cache =  client.getOrCreateCache(new CacheConfiguration<Integer, String>()
                .setName("Test")
                .setBackups(1)
            );
            cache.put(1, "foo");
            System.out.println(">>>    " + cache.get(1));

//            BigDecimal treshhold = new BigDecimal(100000.0);
//            Object res = client.compute().execute(SumBalancesForAllClientsTask2.class, treshhold);
//
//
//            System.out.println(">>>    " + res);

    }

}
