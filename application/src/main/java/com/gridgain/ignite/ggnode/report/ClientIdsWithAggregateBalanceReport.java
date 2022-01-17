package com.gridgain.ignite.ggnode.report;

import com.gridgain.ignite.ggnode.dao.AccountDao;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Created By GridGain Systems
 *
 * Class serves as a reporting client for Inventory items.
 */
public class ClientIdsWithAggregateBalanceReport {

    private static final Log log = LogFactory.getLog(ClientIdsWithAggregateBalanceReport.class);

    public static void main(String[] args) {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        Ignition.setClientMode(true);
        try (Ignite start = Ignition.start("client-config.xml")) {
            reportClientIdsWithAggregateBalance("<", BigDecimal.valueOf(10000));
        }
    }

    public static void reportClientIdsWithAggregateBalance(String op, BigDecimal balance) {
        log.info("**************************************************  REPORT HEADER ***************************************************************************************");
        log.info(String.format("*** Started report: ClientIdsWithAggregateBalance %s %,.2f", op, balance));

        try (AccountDao accountDao = new AccountDao()) {
            List<Map.Entry<Integer, BigDecimal>> hits = accountDao.getClientIdsAndAggregateBalanceFor(op, balance);
            int hitCount = hits == null ? 0 : hits.size();
            log.info(String.format("*** ClientIdsWithAggregateBalance %s %,.2f found %d clients.", op, balance, hitCount));
            if (hitCount > 0) {
                for (Map.Entry<Integer, BigDecimal> e : hits) {
                    log.info(String.format("*** ClientId: %d, Aggregate Balance: %,.2f", (Integer) e.getKey(), (BigDecimal) e.getValue()));
                }
            }
        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }

        log.info("**************************************************  REPORT FOOTER ***************************************************************************************");
    }

}

