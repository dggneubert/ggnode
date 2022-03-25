package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.*;
import org.apache.ignite.cache.query.SqlFieldsQuery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class SumBalancesForClientTaskExecutor {
    private static final Log log = LogFactory.getLog(SumBalancesForClientTaskExecutor.class);
    private static int defaultMinClientId = 1;
    private static int defaultMaxClientId = 100;

    public static void main(String[] args) {

        int minClientId = (args.length == 0) ? defaultMinClientId : Integer.parseInt(args[0]);
        int maxClientId = (args.length <= 1) ? defaultMaxClientId : Integer.parseInt(args[1]);
        boolean useBinaryTask = (args.length == 3 && args[2].equalsIgnoreCase("binary"));

        // Set "quiet" and "client" operation modes for this node
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("client-config.xml")) {

            log.info(String.format("*** SumBalancesForClientTaskExecutor -> initiated for clients with ids: %d .. %d", minClientId, maxClientId));

            // IgniteCompute computeNodes = Ignition.ignite().compute(ignite.cluster().forServers());
            IgniteCluster cluster = ignite.cluster();
            IgniteCompute computeNodes = ignite.compute(cluster.forDataNodes("CLIENT_CACHE"));

            // Set the SQL query to sum the balances of all accounts for the client with give clientIdt
            for (int id = minClientId; id <= maxClientId; id++) {

                var sum = useBinaryTask
                        ? computeNodes.affinityCall(Client.CACHE_NAME, id, new SumBalancesForClientTaskBinary(id))
                        : computeNodes.affinityCall(Client.CACHE_NAME, id, new SumBalancesForClientTask(id));

                if (sum != null)
                    log.info(String.format("*** SumBalancesForClientTaskExecutor -> Client (id=%d) aggregate balance is: %,.2f", id, sum));
                else
                    log.warn(String.format("*** SumBalancesForClientTaskExecutor ->  No account balances were found for client (id=%d).", id));
            }

            log.info("*** FindAggregateBalancesTaskExecutor -> Done ");

        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
            ex.printStackTrace();
        }
    }
}

