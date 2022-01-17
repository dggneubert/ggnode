package com.gridgain.ignite.ggnode.dao;

import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;

import javax.cache.Cache;
import java.util.*;

public class ClientDao implements AutoCloseable{

    public static final String CLIENT_CACHE_NAME = "CLIENT_CACHE";

    private final IgniteCache<Integer, Client> clientCache;

    public ClientDao() {
        Ignite igClient = Ignition.ignite();
        this.clientCache = igClient.cache(CLIENT_CACHE_NAME);
    }

    public void save(Client client) {

        clientCache.put(client.getId( ), client);
    }

    public void saveAll(Map<Integer,Client> clients) {

        clientCache.putAll(clients);
    }

    public Client getClient(int clientId){

        return clientCache.get(clientId);
    }

    public Map<Integer,Client> getClients(Set<Integer> clientIds){
        return clientCache.getAll(clientIds);
    }

    public Integer getClientCount(){
        return clientCache.size(CachePeekMode.PRIMARY);
    }

    public void close(){
        clientCache.close();
    }



    // SQL Queries

    public Set<Integer> getAllClientKeys() {
        HashSet<Integer> clientKeys=new HashSet<Integer>();
        ScanQuery<Integer, Client> scanQuery = new ScanQuery<>();

        try (QueryCursor<Cache.Entry<Integer, Client>> cursor = clientCache.query(scanQuery)) {
            for (Cache.Entry<Integer, Client> entry : cursor)
                clientKeys.add(entry.getKey());
        }
        return clientKeys;
    }

    public List<Client> getClientsWithStatus(String status) {
        String whereClause = " status = ? ";
        SqlQuery<AffinityKey<Integer>, Client> query = new SqlQuery<>(Client.class, whereClause);

        query.setArgs(status);
        List<Client> result = new ArrayList<>();

        try (QueryCursor<Cache.Entry<AffinityKey<Integer>, Client>> cursor = clientCache.query(query)) {
            for (Cache.Entry<AffinityKey<Integer>, Client> entry : cursor)
                result.add(entry.getValue());
        }
        return result;
    }


    // SQL DML
    // Update method
    public Long updateClientStatus (Integer id, String newStatus, boolean localQuery) {
        SqlFieldsQuery query = new SqlFieldsQuery("UPDATE Client set status = ? WHERE id = ?");
        if (localQuery){
            query.setLocal(true);
        }
        query.setArgs(newStatus,id);

        Long result=null;
        // inventoryCache.query(query);
        try (QueryCursor<List<?>> cursor = clientCache.query(query)) {
            for (List<?> row : cursor){
                result = (Long) row.get(0);
            }
        }

        return result;
    }

}

