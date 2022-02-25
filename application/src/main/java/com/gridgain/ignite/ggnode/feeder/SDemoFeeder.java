package com.gridgain.ignite.ggnode.feeder;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;

public class SDemoFeeder {

    public static final int SDEMO_NUM_CLIENTS = 1000000;              // 1M clients (default)
    public static final int SDEMO_NUM_ACCOUNTS_PER_CLIENT = 10000;    // 10K accounts per client (default)
    // public static final String SDEMO_SPRING_CFG_PATH = "client-config.xml";
    public static final String SDEMO_SPRING_CFG_PATH = "C:\\clients\\Schwab\\demo\\gg\\config\\nebula-client-config.xml";

    public static void main(String[] args) throws NumberFormatException {

        // Set any specified runtime arguments: numClients, numAccountsPerClient, and sprigCfgPath
        int numClients = args.length > 0 ? Integer.parseInt(args[0]) : SDEMO_NUM_CLIENTS;
        int numAccountsPerClient = args.length > 1 ? Integer.parseInt(args[1]) : SDEMO_NUM_ACCOUNTS_PER_CLIENT;
        String springCfgPath = (args.length <= 2 || args[2] == null || args[2].isBlank()) ? SDEMO_SPRING_CFG_PATH : args[2].trim();

        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");
        Ignition.setClientMode(true);

        try (Ignite start = Ignition.start(springCfgPath)) {
            // ClientFeeder.loadSDemoClients(numClients);
            ClientAndAccountsFeeder.loadSDemoAccounts(numClients, numAccountsPerClient);
        }
    }
}

