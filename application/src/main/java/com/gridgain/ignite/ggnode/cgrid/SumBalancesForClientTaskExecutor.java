package com.gridgain.ignite.ggnode.cgrid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.*;

import java.math.BigDecimal;

public class SumBalancesForClientTaskExecutor {
    private static final Log log = LogFactory.getLog(SumBalancesForClientTaskExecutor.class);
    private static int defaultClientId = 1;

    public static void main(String[] args) {

        int clientId = (args.length == 0) ? defaultClientId : Integer.parseInt(args[0]);
        boolean useBinaryTask = (args.length == 2 && args[1].equalsIgnoreCase("binary"));

        // Set "quiet" and "client" operation modes for this node
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("client-config.xml")) {
            log.info(String.format("*** SumBalancesForClientTaskExecutor -> initiated for client with id: %d", clientId));

            IgniteCompute computeNodes = Ignition.ignite().compute(ignite.cluster().forServers());

            BigDecimal sum = useBinaryTask
                ? computeNodes.call(new SumBalancesForClientTaskBinary(clientId))
                : computeNodes.call(new SumBalancesForClientTask(clientId));

            if (sum != null)
                log.info(String.format("*** SumBalancesForClientTaskExecutor -> Client (id=%d) aggregate balance on this node: %,.2f", clientId, sum));
            else
                log.warn(String.format("*** SumBalancesForClientTaskExecutor ->  No account balances were found for client (id=%d) on this node.", clientId));

            log.info("*** FindAggregateBalancesTaskExecutor -> Done ");

        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
            ex.printStackTrace();
        }
    }
}

