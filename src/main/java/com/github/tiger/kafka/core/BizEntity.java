package com.github.tiger.kafka.core;

/**
 * @author liuhongming
 */
public class BizEntity {

    /**
     * 业务名称
     */
    private String biz;

    /**
     * 主题名称
     */
    private String topic;

    /**
     * 消息媒介
     */
    private String medium;

    /**
     * 媒介地址
     */
    private String coordinator;

    /**
     * 消费数量
     */
    private int nconsumer;

    /**
     * 后端地址
     */
    private String backendurl;

    /**
     * 后端参数
     */
    private String backendparams;

    /**
     * 变更版本
     */
    private int version;

    public String getBiz() {
        return biz;
    }

    public void setBiz(String biz) {
        this.biz = biz;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMedium() {
        return medium;
    }

    public void setMedium(String medium) {
        this.medium = medium;
    }

    public String getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(String coordinator) {
        this.coordinator = coordinator;
    }

    public int getNconsumer() {
        return nconsumer;
    }

    public void setNconsumer(int nconsumer) {
        this.nconsumer = nconsumer;
    }

    public String getBackendurl() {
        return backendurl;
    }

    public void setBackendurl(String backendurl) {
        this.backendurl = backendurl;
    }

    public String getBackendparams() {
        return backendparams;
    }

    public void setBackendparams(String backendparams) {
        this.backendparams = backendparams;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
