package com.gridgain.ignite.ggnode.model.entities;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Created By GridGain Systems
 * Author: David Neubert
 *
 * Class contains client details that will be kept in the grid.
 */
public class Client {

    public static final String[] Status = {"Standard", "Bronze", "Silver", "Gold", "Platinum"};

    @QuerySqlField(index=true) private final int id;
    @QuerySqlField(index=false) private String name;
    @QuerySqlField(index=true) private String status;

    public Client(int id, String name)
    {
        this.id = id;
        this.name = name;
        this.status = Status[0];
    }

    public Client(int id, String name, String status)
    {
        this.id = id;
        this.name = name;
        this.status = status;
    }

    public int getId( )
    {
        return id;
    }

    public String getName( )
    {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus( )
    {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Client{" +
                "id=" + id +
                ", name='" + name +
                ", status=" + status +
                '}';
    }
}

