package com.gridgain.ignite.ggnode.report;

import com.gridgain.ignite.ggnode.dao.AccountDao;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Created By GridGain Systems
 *
 * Class serves as a reporting client for Inventory items.
 */
public class ClientsWithAggregateBalanceReport {

    private static final Log log = LogFactory.getLog(ClientsWithAggregateBalanceReport.class);

    public static void main(String[] args) {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        Ignition.setClientMode(true);
        try (Ignite start = Ignition.start("client-config.xml")) {
            reportClientsWithAggregateBalance("<", BigDecimal.valueOf(0));
        }
    }

    public static void reportClientsWithAggregateBalance(String op, BigDecimal balance) {
        log.info("**************************************************  REPORT HEADER ***************************************************************************************");
        log.info(String.format("*** Started report: ClientsWithAggregateBalance %s %,.2f", op, balance));

        try (AccountDao accountDao = new AccountDao()) {
            HashMap<Map.Entry<Integer, String>, BigDecimal> hits = accountDao.getClientsAndAggregateBalanceFor(op, balance);
            int hitCount = hits == null ? 0 : hits.size();
            log.info(String.format("*** ClientWithAggregateBalance %s %,.2f found %d clients.", op, balance, hitCount));
            if (hitCount > 0) hits.forEach( (k, v) ->
                    log.info(String.format("*** Client [%d, %s] aggregate balance is: %,.2f", (Integer) k.getKey(), k.getValue(), (BigDecimal) v)));
        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }

        log.info("**************************************************  REPORT FOOTER ***************************************************************************************");
    }

}

