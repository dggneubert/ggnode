package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.resources.IgniteInstanceResource;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteCallable;

import java.math.BigDecimal;


public class SumBalancesForClientTask implements IgniteCallable<BigDecimal> {

    private static final Log log = LogFactory.getLog(SumBalancesForClientTask.class);

    @IgniteInstanceResource
    Ignite igClient;

    int clientId;
    BigDecimal sum;

    public SumBalancesForClientTask(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public BigDecimal call() throws Exception {
        log.debug(String.format("SumBalancesForClientTask -> initiated for client id: %d", clientId));

        sum = null;
        try {
            IgniteCache<Integer, Account> accountCache = igClient.cache("ACCOUNT_CACHE");

            // Specify the SQL query
            SqlFieldsQuery sql = new SqlFieldsQuery("SELECT SUM(balance) FROM ACCOUNT WHERE clientId = ?")
                    .setArgs(clientId)
                    .setLocal(true);

            // Execute the SQL query and return column(0) or row(0), as sql query returns only one (sum) value.
            sum = (BigDecimal)accountCache.query(sql).getAll().get(0).get(0);

            log.debug(String.format("SumBalancesForClientTask -> Client (id=%d) aggregate balance: %,.2f", clientId, sum));

            accountCache.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        log.debug("SumBalancesForClientTask -> Completed ");
        return sum;
    }
}
