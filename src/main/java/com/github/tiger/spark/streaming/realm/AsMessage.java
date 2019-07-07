package com.github.tiger.spark.streaming.realm;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author liuhongming
 */
public class AsMessage implements Serializable {

    private Long writeTime;
    private String traceId;
    private String[] condition;
    private String keyword = "";
    private String nowLocation;

    public Long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(Long writeTime) {
        this.writeTime = writeTime;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String[] getCondition() {
        return condition;
    }

    public void setCondition(String[] condition) {
        this.condition = condition;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getNowLocation() {
        return nowLocation;
    }

    public void setNowLocation(String nowLocation) {
        this.nowLocation = nowLocation;
    }

    @Override
    public String toString() {
        return "AsMessage{" +
                "writeTime=" + writeTime +
                ", traceId='" + traceId + '\'' +
                ", condition=" + Arrays.toString(condition) +
                ", keyword='" + keyword + '\'' +
                ", nowLocation='" + nowLocation + '\'' +
                '}';
    }
}
