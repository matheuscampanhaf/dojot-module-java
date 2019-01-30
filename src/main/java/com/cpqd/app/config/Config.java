package com.cpqd.app.config;

import java.lang.System;

public class Config {

    private static Config mInstance;

    private String mDeviceManagerAddress;
    private String mImageManagerAddress;
    private String mAuthAddress;
    private String mDataBrokerAddress;
    private String mKafkaAddress;
    private String mInternalDefaultTenant;
    private String mDeviceManagerDefaultSubject;
    private String mDeviceManagerDefaultManager;
    private String mTenancyManagerDefaultSubject;
    private String mTenancyManagerDefaultManager;
    private String mIotagentDefaultSubject;
    private String mKafkaDefaultManager;
    private Integer mKafkaDefaultSessionTimeout;
    private String mKafkaDefaultGroupId;

    private Config() {
        if (System.getenv("DEVM_ADDRESS") != null) {
            this.mDeviceManagerAddress = System.getenv("DEVM_ADDRESS");
        } else {
            // this.mDeviceManagerAddress = "http://172.20.0.21:5000";
            this.mDeviceManagerAddress = "http://device-manager:5000";
        }

        if (System.getenv("IMGM_ADDRESS") != null) {
            this.mImageManagerAddress = System.getenv("IMGM_ADDRESS");
        } else {
            this.mImageManagerAddress = "image-manager:5000";
        }

        if (System.getenv("AUTH_ADDRESS") != null) {
            this.mAuthAddress = System.getenv("AUTH_ADDRESS");
        } else {
            // this.mAuthAddress = "http://172.20.0.22:5000";
            this.mAuthAddress = "http://auth:5000";
        }

        if (System.getenv("DATA_BROKER_ADDRESS") != null) {
            this.mDataBrokerAddress = System.getenv("DATA_BROKER_ADDRESS");
        } else {
            // this.mDataBrokerAddress = "http://172.20.0.18:80";
            this.mDataBrokerAddress = "http://data-broker:80";
        }

        if (System.getenv("KAFKA_ADDRESS") != null) {
            this.mKafkaAddress = System.getenv("KAFKA_ADDRESS");
        } else {
            // this.mKafkaAddress = "172.20.0.16:9092";
            this.mKafkaAddress = "kafka:9092";
        }

        this.mInternalDefaultTenant = "internal";

        this.mDeviceManagerDefaultSubject = "dojot.device-manager.device";
        this.mDeviceManagerDefaultManager = "http://" + this.mDeviceManagerAddress;

        this.mTenancyManagerDefaultSubject = "dojot.tenancy";
        this.mTenancyManagerDefaultManager = "http://" + this.mAuthAddress;

        this.mIotagentDefaultSubject = "device-data";

        this.mKafkaDefaultManager = "http://" + this.mKafkaAddress;
        this.mKafkaDefaultSessionTimeout = 15000;

        Integer randomNumber = (int)(Math.random() * 10000 + 1);
        this.mKafkaDefaultGroupId = "dojot-module-java-" + randomNumber.toString();
    }

    public static synchronized Config getInstance() {
        if (mInstance == null) {
            mInstance = new Config();
        }
        return mInstance;
    }

    public String getInternalTenant(){
        return this.mInternalDefaultTenant;
    }

    public String getDeviceManagerAddress() {
        return this.mDeviceManagerAddress;
    }

    public String getImageManagerAddress() {
        return this.mImageManagerAddress;
    }

    public String getAuthAddress() {
        return this.mAuthAddress;
    }

    public String getDataBrokerAddress() {
        return this.mDataBrokerAddress;
    }

    public String getKafkaAddress() {
        return this.mKafkaAddress;
    }

    public String getDeviceManagerDefaultSubject() {
        return this.mDeviceManagerDefaultSubject;
    }

    public String getDeviceManagerDefaultManager() {
        return this.mDeviceManagerDefaultManager;
    }

    public String getTenancyManagerDefaultSubject() {
        return this.mTenancyManagerDefaultSubject;
    }

    public String getTenancyManagerDefaultManager() {
        return this.mTenancyManagerDefaultManager;
    }

    public String getIotagentDefaultSubject() {
        return this.mIotagentDefaultSubject;
    }

    public String getKafkaDefaultManager() {
        return this.mKafkaDefaultManager;
    }

    public Integer getKafkaDefaultSessionTimeout() {
        return this.mKafkaDefaultSessionTimeout;
    }

    public String getKafkaDefaultGroupId() {
        return this.mKafkaDefaultGroupId;
    }
}