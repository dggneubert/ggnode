package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.resources.IgniteInstanceResource;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteCallable;

import java.math.BigDecimal;
import java.util.List;


public class SumBalancesForClientTask implements IgniteCallable<BigDecimal> {

    private static final Log log = LogFactory.getLog(SumBalancesForClientTask.class);

    @IgniteInstanceResource
    Ignite _ignite;

    int clientId;

    public SumBalancesForClientTask(int clientId)
    {
        this.clientId = clientId;
    }

    @Override
    public BigDecimal call() throws Exception {
        log.debug(String.format("SumBalancesForClientTask: initiated for client id: %d", clientId));

        BigDecimal sum = null;
        var accountCfg = Account.getCacheConfiguration();

        try (var accountCache = _ignite.getOrCreateCache(accountCfg))
        {
            IgniteCache<BinaryObject, BinaryObject> accountCacheKB = accountCache.withKeepBinary();

            // Set the SQL query to sum the balances of all accounts for the client with give clientId.
            // Run this query only on the local node that is executing this task, i.e. setLocal(true).
            SqlFieldsQuery sql = new SqlFieldsQuery("SELECT SUM(balance) FROM Account WHERE clientId = ?")
                    .setArgs(clientId)
                    .setLocal(true);

            // Execute SQL query and return the aggregated sum balance.
            List<List<?>> result = accountCacheKB.query(sql).getAll();
            sum = (BigDecimal)result.get(0).get(0);

            log.debug(String.format("SumBalancesForClientTask: Client (id=%d) aggregate balance: %,.2f", clientId, sum));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sum;
    }
}
