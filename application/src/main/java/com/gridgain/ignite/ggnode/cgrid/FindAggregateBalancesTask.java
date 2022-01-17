package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import org.apache.ignite.resources.IgniteInstanceResource;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteCallable;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FindAggregateBalancesTask implements IgniteCallable<List<Map.Entry<Integer, Long>>> {
    @IgniteInstanceResource
    Ignite igClient;
    String balanceOp;
    long balance;

    public FindAggregateBalancesTask(String balanceOp, long balance) {
        this.balanceOp = balanceOp;
        this.balance = balance;
    }

    @Override
    public List<Map.Entry<Integer, Long>> call() throws Exception {
        System.out.println(String.format("FindAggregateBalanceTask -> Call initiated for balances %s %s.", balanceOp, String.valueOf(balance)));
        List<Map.Entry<Integer, Long>> found = new ArrayList<>();

        try {
            IgniteCache<Integer, Account> accountCache = igClient.cache("ACCOUNT_CACHE");


            // Set up the SQL query
            String sq = String.format("SELECT clientId, SUM(balance) FROM ACCOUNT GROUP BY clientId HAVING SUM(balance) %s %d ORDER BY clientId", balanceOp, balance);
            SqlFieldsQuery sql = new SqlFieldsQuery(sq);
            // sql.setArgs(balance);
            sql.setLocal(true);

            // Execute the query and obtain the query result cursor.
            try (QueryCursor<List<?>> cursor = accountCache.query(sql)) {
                for (List<?> row : cursor) {
                    found.add(new AbstractMap.SimpleEntry((Integer) row.get(0), ((BigDecimal) row.get(1)).longValue()));
                }
            }

            System.out.println(String.format("FindAggregateBalancesTask -> found %d clients with aggregate balances %s %d", found.size(), balanceOp, balance));

            accountCache.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        System.out.println("FindAggregateBalancesTask -> Completed ");
        return found;

    }
}
