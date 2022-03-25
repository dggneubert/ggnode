package com.gridgain.ignite.ggnode.model.entities;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class Account implements Serializable {

    public static final String SQL_SCHEMA = "SDEMO";
    public static final String CACHE_NAME = "ACCOUNT_CACHE";

    @QuerySqlField private String name;
    @QuerySqlField private Integer type;
    @QuerySqlField private BigDecimal balance;

    public static CacheConfiguration<AccountKey, Account> getCacheConfiguration()
    {
        CacheConfiguration<AccountKey, Account> cfg = new CacheConfiguration();
        cfg.setSqlSchema(SQL_SCHEMA);
        cfg.setName(CACHE_NAME);
        cfg.setBackups(0);
        cfg.setQueryEntities(GetCacheQueryEntities());
        return cfg;
    }

    public static List<QueryEntity> GetCacheQueryEntities() {

        QueryEntity queryEntity = new QueryEntity(AccountKey.class, Account.class);
        return Arrays.asList(queryEntity);
    }

    public Account(String name) {
        this.name = name;
        this.type = 0;
        this.balance = new BigDecimal("0.00");
    }

    public Account(String name, int type, BigDecimal balance) {
        this.name = name;
        this.type = type;
        this.balance = balance;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public Integer getType() {
        return type;
    }
    void setType(Integer type) {
        this.type = type;
    }

    public BigDecimal getBalance() {
        return balance;
    }
    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    @Override
    public String toString() {
        return String.format("Account[Name=%s, Type=%d, Balance=%,.2f]", name, type, balance);
    }
}
