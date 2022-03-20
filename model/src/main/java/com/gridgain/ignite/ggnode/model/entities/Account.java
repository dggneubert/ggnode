package com.gridgain.ignite.ggnode.model.entities;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Arrays;
import java.util.List;

/**
 * Created By GridGain Systems
 *
 * Class contains PurchaseOrder details that will be kept in the grid as an embedded class.
 */


public class Account {

    public static final String SQL_SCHEMA = "SDEMO";
    public static final String CACHE_NAME = "ACCOUNT_CACHE";

    @QuerySqlField private String name;
    @QuerySqlField private Integer type;
    @QuerySqlField private Long balance;
    @QuerySqlField private String status;

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
        this.balance = 0L;
        this.status = "NEW";
    }

    public Account(String name, int type, long balance, String status) {
        this.name = name;
        this.type = type;
        this.balance = balance;
        this.status = status;
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

    public void setType(Integer type) {
        this.type = type;
    }

    public Long getBalance() {
        return balance;
    }

    public void setBalance(Long balance) {
        this.balance = balance;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return String.format("Account [Name = %s, Type = %d, Balance = %d, Status = %s]", name, type, balance, status);
    }
}
