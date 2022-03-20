package com.gridgain.ignite.ggnode.node;

import com.gridgain.ignite.ggnode.cgrid.GenerateAccountsTask;
import com.gridgain.ignite.ggnode.cgrid.GenerateClientsTask;
import com.gridgain.ignite.ggnode.feeder.ClientAndAccountsFeeder;
import com.gridgain.ignite.ggnode.model.entities.Account;
import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.*;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SDemoClientNode {

    static final Log log = LogFactory.getLog(SDemoClientNode.class);

    static final String CFG_PATH = "C:\\clients\\Schwab\\demo\\gg\\config\\local-java-server-config.xml";
    // static final String CFG_PATH = Paths.get(System.getenv("SCHWAB_DEMO_GG_CONFIG"), "server-config.xml").toString();

    // SDemoClientNode default values
    static final int DEFAULT_NUM_CLIENTS = 1_000_000;
    static final int DEFAULT_NUM_ACCOUNTS_PER_CLIENT = 10; // 10_000;
    static final int DEFAULT_CLIENT_ID = 1;
    static final BigDecimal DEFAULT_AGGR_BALANCE_LIMIT = new BigDecimal(100000);
    static final int DEFAULT_NUM_PROCESSORS_PER_DATA_NODE = 3; // 8;

    public static void main(String[] args) throws NumberFormatException {

        // Initialize program argument runtime values
        int numClients = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_NUM_CLIENTS;
        int numAccountsPerClient = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_NUM_ACCOUNTS_PER_CLIENT;
        int clientId = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_CLIENT_ID;
        BigDecimal aggrBalanceLimit = args.length > 3 ? new BigDecimal(args[3]) : DEFAULT_AGGR_BALANCE_LIMIT;
        int numProcessorsPerDataNode = args.length > 4 ? Integer.parseInt(args[4]) : DEFAULT_NUM_PROCESSORS_PER_DATA_NODE;

        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("H:mm:ss");

        // Initialize Ignite properties
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start(CFG_PATH)) {

            log.info(String.format("*** SDemoClientNode started using xml config: %s", CFG_PATH));

            // Destroy previous caches -- start caches from scratch every time
            ignite.destroyCache(Client.CACHE_NAME);
            ignite.destroyCache(Account.CACHE_NAME);

            // Create Client cache
            var clientCfg = Client.getCacheConfiguration();
            var clientCache = ignite.getOrCreateCache(clientCfg);

            // Create Account cache
            var accountCfg = Account.getCacheConfiguration();
            var accountCache = ignite.getOrCreateCache(accountCfg);

            // Get the compute-grid for the Client cache data nodes
            IgniteCluster cluster = ignite.cluster();
            IgniteCompute compute = ignite.compute(cluster.forDataNodes(clientCache.getName()));

            // Calculate the total number of jobs required (based upon number of nodes and cpus/node)
            var nodeCount = ignite.cluster().nodes().size() - 1;
            var jobCount = nodeCount * numProcessorsPerDataNode;

            // Calculate the number and size of batches needed to distribute and populate clients in the compute-grid.
            var batchCount = numClients / jobCount + 1;
            var batchSize = numClients / batchCount;

            // ***********  Populate and test Client records using the Client nodes compute grid  *********************
            log.info(String.format("*** Begin CLIENT generation using cache %s: %s", clientCache.getName(), LocalDateTime.now().format(timeFormatter)));
            compute.run(GenerateClientsTask.BuildTaskBatches(numClients, clientCache.getName(), batchCount, batchSize));
            log.info(String.format("*** Begin CLIENT testing using cache %s: %s", clientCache.getName(), LocalDateTime.now().format(timeFormatter)));
            GenerateClientsTask.TestUsing(clientCache, 1000);
            log.info(String.format("*** End CLIENT generation/test: %s", LocalDateTime.now().format(timeFormatter)));

            // ***********   Populate and test Account records using the compute grid  ******************************
            log.info(String.format("*** Begin ACCOUNT generation using cache %s: %s", accountCache.getName(), LocalDateTime.now().format(timeFormatter)));
            compute.run(GenerateAccountsTask.BuildTaskBatches(numClients, accountCache.getName(), numAccountsPerClient, batchCount, batchSize));
            log.info(String.format("*** Begin ACCOUNT testing using cache %s: %s", accountCache.getName(), LocalDateTime.now().format(timeFormatter)));
            GenerateAccountsTask.TestUsing(accountCache, 1000);
            log.info(String.format("*** End ACCOUNT generation/test: %s", LocalDateTime.now().format(timeFormatter)));

            // Emd SDemoClient
            log.info(String.format("*** SDemoClientNode completed at : %s", LocalDateTime.now().format(timeFormatter)));

        }
    }
}

