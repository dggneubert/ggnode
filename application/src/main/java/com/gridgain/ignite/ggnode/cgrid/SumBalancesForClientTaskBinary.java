package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.resources.IgniteInstanceResource;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteCallable;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.*;


public class SumBalancesForClientTaskBinary implements IgniteCallable<BigDecimal> {

    private static final Log log = LogFactory.getLog(SumBalancesForClientTaskBinary.class);

    @IgniteInstanceResource
    Ignite ignite;
    int clientId;

    public SumBalancesForClientTaskBinary(int clientId) {
        this.clientId = clientId;
    }

    @Nullable
    @Override
    public BigDecimal call() throws Exception {
        log.debug(String.format("SumBalancesForClientTaskBinary -> initiated for client id: %d", clientId));

        // Set SQL query to return the 'balance' field from all rows having the specified clientId
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT balance FROM ACCOUNT WHERE clientId = ?")
                .setArgs(clientId)
                .setLocal(true);

        long sum = 0L;
        boolean isSumValid = false;
        try {
            IgniteCache<Integer, Account> accountCache = ignite.cache("ACCOUNT_CACHE");
            IgniteCache<BinaryObject, BinaryObject> accountCacheKB = accountCache.withKeepBinary();

            QueryCursor<List<?>> accounts = accountCacheKB.query(qry);

            for (List<?> row : accounts.getAll()) {
                sum += (Long) row.get(0);
                isSumValid = true;
            }

            // accountCacheKB.close();  TODO should KeepBinary copy be closed?
            accountCache.close();

        } catch (Exception ex) {
            ex.printStackTrace();
            isSumValid = false;
        }

        log.debug("SumBalancesForClientTaskBinary -> Completed ");
        return isSumValid ? new BigDecimal(sum) : null;
    }
}
