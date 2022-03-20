package com.gridgain.ignite.ggnode.model.entities;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created By GridGain Systems
 * Author: David Neubert
 *
 * Class contains client details that will be kept in the grid.
 */
public class Client implements Serializable {

    private String name;
    private String status;

    public static final String SQL_SCHEMA = "SDEMO";
    public static final String CACHE_NAME = "CLIENT_CACHE";
    public static final String[] STATUS = {"Standard", "Bronze", "Silver", "Gold", "Platinum"};

    public static CacheConfiguration<Long, Client> getCacheConfiguration()
    {
        CacheConfiguration<Long, Client> cfg = new CacheConfiguration<Long, Client>();
        cfg.setSqlSchema(SQL_SCHEMA);
        cfg.setName(CACHE_NAME);
        cfg.setBackups(0);
        cfg.setQueryEntities(GetCacheQueryEntities());
        return cfg;
    }

    public static List<QueryEntity> GetCacheQueryEntities() {

        QueryEntity queryEntity = new QueryEntity(Long.class, Client.class)
                .setKeyFieldName("id")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("name", String.class.getName(), null)
                .addQueryField("status", String.class.getName(), null)
                .setIndexes(Arrays.asList(new QueryIndex("id")));

        return Arrays.asList(queryEntity);
    }

    public Client(String name)
    {
        this.name = name;
        this.status = STATUS[0];
    }

    public Client(String name, String status)
    {
        this.name = name;
        this.status = status;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    @Override
    public String toString()
    {
        return String.format("Client [Name = %s, Status = %s]", name, status);
    }
}

