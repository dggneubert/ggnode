package com.gridgain.ignite.ggnode.model.entities;

import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Created By GridGain Systems
 *
 * Class contains PurchaseOrder details that will be kept in the grid as an embedded class.
 */
public class Account {

    @QuerySqlField(index = true) private Integer id;
    @QuerySqlField(index = true) private Integer clientId;
    @QuerySqlField private String name;
    @QuerySqlField private Integer type;
    @QuerySqlField private Long balance;
    @QuerySqlField private String status;


    public Account(Integer id, Integer clientId, String name) {
        this.id = id;
        this.clientId = clientId;
        this.name = name;
        this.type = 0;
        this.balance = 0L;
        this.status = "NEW";
    }

    public Account(Integer id, Integer clientId, String name, Integer type) {
        this.id = id;
        this.clientId = clientId;
        this.name = name;
        this.type = type;
        this.balance = 0L;
        this.status = "NEW";
    }

    public Account(Integer id, Integer clientId, String name, Integer type, Long balance) {
        this.id = id;
        this.clientId = clientId;
        this.name = name;
        this.type = type;
        this.balance = balance;
        this.status = "NEW";
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getClientId() {
        return clientId;
    }

    public void setClientId(Integer clientId) {
        this.clientId = clientId;
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
        return "Account{" +
                "id=" + id +
                ", clientId=" + clientId +
                ", name=" + name +
                ", type=" + type +
                ", balance=" + balance +
                ", status='" + status + '\'' +
                '}';
    }
}
