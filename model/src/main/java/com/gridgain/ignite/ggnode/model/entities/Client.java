package com.gridgain.ignite.ggnode.model.entities;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


public class Client implements Serializable {

    private String name;
    private Integer level;

    public static final String SQL_SCHEMA = "SDEMO";
    public static final String CACHE_NAME = "CLIENT_CACHE";
    public static final String[] LEVELS = {"Bronze", "Silver", "Gold", "Platinum"};

    public static CacheConfiguration<Long, Client> getCacheConfiguration()
    {
        return new CacheConfiguration<Long, Client>()
                .setSqlSchema(SQL_SCHEMA)
                .setName(CACHE_NAME)
                .setBackups(0)
                .setQueryEntities(GetCacheQueryEntities());
    }

    public static List<QueryEntity> GetCacheQueryEntities() {

        QueryEntity queryEntity = new QueryEntity(Long.class, Client.class)
                .setKeyFieldName("id")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("name", String.class.getName(), null)
                .addQueryField("level", Integer.class.getName(), null)
                .setIndexes(Arrays.asList(new QueryIndex("id")));

        return Arrays.asList(queryEntity);
    }

    public Client(String name)
    {
        this.name = name;
        this.level = 0;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Integer getLevel() { return level; }
    public void setLevel(Integer level) { this.level = level; }

    @Override
    public String toString()
    {
        return String.format("Client[name=%s, level=%s]", name, LEVELS[level]);
    }
}

