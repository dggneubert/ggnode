package com.gridgain.ignite.ggnode.dao;

import com.gridgain.ignite.ggnode.model.entities.Account;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

import java.math.BigDecimal;
import java.util.*;


public class AccountDao implements AutoCloseable {

    private static final String ACCOUNT_CACHE_NAME = "ACCOUNT_CACHE";

    private final IgniteCache<AffinityKey<Integer>, Account> accountCache;
    private final IgniteAtomicSequence accountSequence;

    public AccountDao(){
        Ignite igClient = Ignition.ignite();
        this.accountCache = igClient.cache(ACCOUNT_CACHE_NAME);
        this.accountSequence = igClient.atomicSequence("AccountSequence", 1 ,true);
    }

    public void save(Account account) {
        AffinityKey<Integer> accountKey = new AffinityKey<>(account.getId(), account.getClientId());
        accountCache.put(accountKey, account);
    }

    public void saveAll(Map<AffinityKey<Integer>, Account> accounts) {
        accountCache.putAll(accounts);
    }

    public Account get(AffinityKey<Integer> accountKey) {
        return accountCache.get(accountKey);
    }

    public int generateAccountId(){
        return (int) accountSequence.getAndIncrement();
    }


    public void close(){
        accountCache.close();
    }


    // SQL Methods

    public Long getAggregateBalanceForClientIdAsLong(Integer clientId) {

        return getAggregateBalanceForClientId(clientId).longValue();
    }

    public BigDecimal getAggregateBalanceForClientId(Integer clientId ) {
        // Initialize and setup the query
        SqlFieldsQuery sql = new SqlFieldsQuery("select sum(balance) from Account where clientId  = ? ");
        sql.setArgs(clientId);

        // Execute the query and obtain the query result cursor.
        BigDecimal result = new BigDecimal(0);
        try (QueryCursor<List<?>> cursor = accountCache.query(sql)) {
            for (List<?> row : cursor){
                result = (BigDecimal) row.get(0);
            }
        }
        return result;
    }

    public Set<Integer> getClientIdsHavingAggregateBalance(String op, BigDecimal balance) {

        // Set up the aggregate SQL query
        SqlFieldsQuery sql = new SqlFieldsQuery(
                "SELECT clientId FROM Account GROUP BY clientId HAVING SUM(balance) " + op + " ?");
        sql.setArgs(balance);

        // Execute the query and build result using the query result cursor.
        Set<Integer> result = new TreeSet<Integer>();
        try (QueryCursor<List<?>> cursor = accountCache.query(sql)) {
            for (List<?> row : cursor) {
                result.add((Integer) row.get(0));
            }
        }
        return result;
    }

    public List<Map.Entry<Integer, BigDecimal>> getClientIdsAndAggregateBalanceFor(String op, BigDecimal balance) {

        // Set up the aggregate SQL query
        SqlFieldsQuery sql = new SqlFieldsQuery("SELECT clientId, SUM(balance)" +
                "    FROM ACCOUNT" +
                "    GROUP BY clientId" +
                "    HAVING SUM(balance) " + op + " ?" +
                "    ORDER BY clientId;");
        sql.setArgs(balance);

        // Execute the query and obtain the query result cursor.
        List<Map.Entry<Integer, BigDecimal>> result = new ArrayList<>();
        try (QueryCursor<List<?>> cursor = accountCache.query(sql)) {
            for (List<?> row : cursor) {
                result.add(new AbstractMap.SimpleEntry((Integer) row.get(0), (BigDecimal) row.get(1)));
            }
        }
        return result;
    }

    public HashMap<Map.Entry<Integer, String>, BigDecimal> getClientsAndAggregateBalanceFor(String op, BigDecimal balance) {

        // Set up the aggregate SQL query
        SqlFieldsQuery sql = new SqlFieldsQuery("SELECT c.id, c.name, a.SUM(balance)" +
                "    FROM CLIENT_CACHE.CLIENT as c, ACCOUNT_CACHE.ACCOUNT as a" +
                "    WHERE c.id = a.clientId" +
                "    GROUP BY c.id" +
                "    HAVING a.SUM(balance) " + op + " ?" +
                "    ORDER BY c.id;");
        sql.setArgs(balance);

        // Execute the query and obtain the query result cursor.
        HashMap<Map.Entry<Integer, String>, BigDecimal> result = new HashMap<Map.Entry<Integer, String>, BigDecimal>();
        try (QueryCursor<List<?>> cursor = accountCache.query(sql)) {
            for (List<?> row : cursor) {
                result.put(new AbstractMap.SimpleEntry<Integer, String>((Integer)row.get(0), row.get(1).toString()), (BigDecimal)row.get(2));
            }
        }
        return result;
    }

    // SQL Query
    // Retrieve order line details per order id
   /* public List<Map<String,String>> findOrderInfoByOrderId(Integer orderId) {
        ArrayList<Map<String,String>> result = new ArrayList<>();

        SqlFieldsQuery sql = new SqlFieldsQuery(
                "select lineNumber , inventoryItemNumber, quantity "
                        + " from OrderLine  "
                        + " where orderId = ?");
        sql.setArgs(orderId);

        // Execute the query and obtain the query result cursor.
        try (QueryCursor<List<?>> cursor = orderLineCache.query(sql)) {
            for (List<?> row : cursor){
                TreeMap<String,String> record = new TreeMap<>();
                record.put("orderLineNumber", ( (Integer) row.get(0)).toString());
                record.put("inventoryItemNumber", ( (Integer) row.get(1)).toString());
                record.put("quantity", ( (Integer) row.get(2)).toString());
                result.add(record);
            }
        }
        return result;
    }*/

}


