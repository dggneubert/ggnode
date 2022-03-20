package com.gridgain.ignite.ggnode.node;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;

public class SDemoServerNode {

    public static void main(String[] args) {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        Ignition.setClientMode(false);
        // In this example in this location we have the server node configuration
        // The requirement is to point to the proper location where the server nodes have their configuration
        // file.

        // String configPath = Paths.get(System.getenv("SCHWAB_DEMO_GG_CONFIG"), "server-config.xml").toString();
        String configPath = "C:\\clients\\Schwab\\demo\\gg\\config\\local-java-server-config.xml";

        try (Ignite ignite = Ignition.start(configPath)) {
            while (true){
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}

