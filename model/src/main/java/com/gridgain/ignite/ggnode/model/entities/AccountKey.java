package com.gridgain.ignite.ggnode.model.entities;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class AccountKey
{
    @QuerySqlField
    public Long id;

    @QuerySqlField
    @AffinityKeyMapped
    public Long clientId;

    public AccountKey(Long id, Long clientId) {
        this.id = id;
        this.clientId = clientId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getClientId() {
        return clientId;
    }

    public void setClientId(Long clientId) {
        this.clientId = clientId;
    }
}
