package com.gridgain.ignite.ggnode.cgrid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;


public class FindAggregateBalancesTaskExecutorJson {
    private static final Log log = LogFactory.getLog(FindAggregateBalancesTaskExecutorJson.class);

    public static void main(String[] args) {

        // Get the aggregate balance operation (<, <=, >, >=) and balance parameters or use defaults
        String balanceOp = (args == null || args.length < 1) ? "<" : args[0];
        Long balanceVal =  (args == null || args.length < 2) ? 10000L : Long.valueOf(args[1]);

        // Set "quiet" and "client" operation modes for this node
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("client-config.xml")) {
            log.info("*** FindAggregateBalancesTaskExecutor -> Run ");

            IgniteCompute computeNodes = Ignition.ignite( ).compute(ignite.cluster().forServers());

            String json = computeNodes.call(new FindAggregateBalancesTaskJson(balanceOp, balanceVal));

            log.info("*** FindAggregateBalancesTaskExecutor found: " + json);
            log.info("*** FindAggregateBalancesTaskExecutor -> Done ");

        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
            ex.printStackTrace();
        }
    }
}


