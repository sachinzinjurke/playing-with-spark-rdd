package com.spark.tutorial.ubs;

import java.sql.Timestamp;

public class Inventory {

    private String inventoryId;
    private String gpnId;
    private Integer version;
    private Timestamp ts;
    private int masterId;
    private String inventoryKey;

    public String getInventoryId() {
        return inventoryId;
    }

    public void setInventoryId(String inventoryId) {
        this.inventoryId = inventoryId;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public String getGpnId() {
        return gpnId;
    }

    public void setGpnId(String gpnId) {
        this.gpnId = gpnId;
    }

    public int getMasterId() {
        return masterId;
    }

    public void setMasterId(int masterId) {
        this.masterId = masterId;
    }

    public String getInventoryKey() {
        return inventoryKey;
    }

    public void setInventoryKey(String inventoryKey) {
        this.inventoryKey = inventoryKey;
    }

    @Override
    public String toString() {
        return "Inventory{" +
                "inventoryId='" + inventoryId + '\'' +
                ", gpnId='" + gpnId + '\'' +
                ", version=" + version +
                ", ts=" + ts +
                ", masterId=" + masterId +
                ", inventoryKey='" + inventoryKey + '\'' +
                '}';
    }
}
