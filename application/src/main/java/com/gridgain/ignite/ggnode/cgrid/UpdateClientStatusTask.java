package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.ignite.resources.IgniteInstanceResource;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteCallable;

import java.math.BigDecimal;
import java.util.List;


public class UpdateClientStatusTask implements IgniteCallable<Boolean> {
    @IgniteInstanceResource
    Ignite igClient;
    Integer clientId;

    public UpdateClientStatusTask(Integer clientId) {
        this.clientId = clientId;
    }

    @Override
    public Boolean call() throws Exception {
        System.out.println("UpdateClientStatusTask -> Run ");

        Client client;
        Integer level = 0;
        long balance = 0;
        boolean updated = false;

        try {
            // Get Client and Account caches
            IgniteCache<Integer, Client> clientCache = igClient.cache("CLIENT_CACHE");
            IgniteCache<AffinityKey<Integer>, Account> accountCache = igClient.cache("ACCOUNT_CACHE");

            // Get Accounts count for Client
            SqlFieldsQuery sql = new SqlFieldsQuery("select sum(balance) from Account where clientId  = ? ");
            sql.setArgs(clientId);
            sql.setLocal(true);

            client = clientCache.get(clientId);
            System.out.println(String.format("UpdateClientStatusTask -> Checking aggregate balance for client (id=%d)", clientId));

            // Execute the query and obtain the query result cursor.
            try (QueryCursor<List<?>> cursor = accountCache.query(sql)) {
                for (List<?> row : cursor) {
                    balance = ((BigDecimal)row.get(0)).longValue();
                    if (balance < 0)
                        level = 0;  // was "In Default";
                    else if (balance >= 0 && balance  < 1000)
                        level = 0; // was "Critical";
                    else if (balance >= 1000 && balance < 10000)
                        level = 0; // was "Bronze";
                    else if (balance >= 10000 && balance < 500000 )
                        level = 1; // was "Silver";
                    else if (balance >= 500000 && balance < 1000000 )
                        level = 2; // was "Gold";
                    else if (balance >= 10000000)
                        level = 3; // was "Platinum";

                    // Update Client status
                    if (level != client.getLevel()) {
                        SqlFieldsQuery query = new SqlFieldsQuery("UPDATE Client set LEVEL = ? WHERE id = ?");
                        query.setLocal(true);
                        query.setArgs(level, clientId);
                        clientCache.query(query);
                        updated=true;
                        System.out.println(String.format("UpdateClientStatusTask -> Client (id=%d) status was updated to %s.",clientId, level));
                    }
                }
            }
            clientCache.close();
            accountCache.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        System.out.println("UpdateClientStatusTask -> Completed ");
        return updated;

    }
}
