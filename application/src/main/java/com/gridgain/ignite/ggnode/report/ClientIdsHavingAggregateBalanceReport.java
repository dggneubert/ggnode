package com.gridgain.ignite.ggnode.report;

import com.gridgain.ignite.ggnode.dao.AccountDao;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Created By GridGain Systems
 *
 * Class serves as a reporting client for Inventory items.
 */
public class ClientIdsHavingAggregateBalanceReport {

    private static final Log log = LogFactory.getLog(ClientIdsHavingAggregateBalanceReport.class);

    public static void main(String[] args) {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        Ignition.setClientMode(true);
        try (Ignite start = Ignition.start("client-config.xml")) {
            // Simple Query
            reportClientIdsHavingAggregateBalance("<", BigDecimal.valueOf(10000));
        }
    }

    public static void reportClientIdsHavingAggregateBalance(String op, BigDecimal balance) {
        log.info("**************************************************  REPORT HEADER ***************************************************************************************");
        log.info("*** Started report: ClientIdsHavingAggregateBalance " + op + " " + balance);
        try (AccountDao accountDao = new AccountDao()) {

            Set<Integer> clientIds = accountDao.getClientIdsHavingAggregateBalance(op, balance);
            if (clientIds!=null && !clientIds.isEmpty()) {
                log.info(String.format("*** ClientIdsHavingAggregateBalance %s %,.2f found %d clientIds.", op, balance, clientIds.size()));
                for (Integer id : clientIds) {
                    log.info("*** " + id.toString());
                }
            } else {
                log.info(String.format("*** ClientsWithStatusReport did not find any clients with aggregate %s %,.2f.", op, balance));
            }

        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }
        log.info("**************************************************  REPORT FOOTER ***************************************************************************************");
    }

}

