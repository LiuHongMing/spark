package com.github.tiger.spark.streaming.realm;

import java.io.Serializable;
import java.util.Arrays;

public class UserBehaviors implements Serializable {

    private String jobNum;
    private String opType;

    private String[] tagSubType;
    private String[] jobType;

    private String cityId;
    private String companySize;
    private String companyType;

    private String campusId;
    private String campusGUID;

    public String getJobNum() {
        return jobNum;
    }

    public void setJobNum(String jobNum) {
        this.jobNum = jobNum;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public String[] getTagSubType() {
        return tagSubType;
    }

    public void setTagSubType(String[] tagSubType) {
        this.tagSubType = tagSubType;
    }

    public String[] getJobType() {
        return jobType;
    }

    public void setJobType(String[] jobType) {
        this.jobType = jobType;
    }

    public String getCityId() {
        return cityId;
    }

    public void setCityId(String cityId) {
        this.cityId = cityId;
    }

    public String getCompanySize() {
        return companySize;
    }

    public void setCompanySize(String companySize) {
        this.companySize = companySize;
    }

    public String getCompanyType() {
        return companyType;
    }

    public void setCompanyType(String companyType) {
        this.companyType = companyType;
    }

    public String getCampusId() {
        return campusId;
    }

    public void setCampusId(String campusId) {
        this.campusId = campusId;
    }

    public String getCampusGUID() {
        return campusGUID;
    }

    public void setCampusGUID(String campusGUID) {
        this.campusGUID = campusGUID;
    }

    @Override
    public String toString() {
        return "UserBehaviors{" +
                "jobNum='" + jobNum + '\'' +
                ", opType='" + opType + '\'' +
                ", tagSubType=" + Arrays.toString(tagSubType) +
                ", jobType=" + Arrays.toString(jobType) +
                ", cityId='" + cityId + '\'' +
                ", companySize='" + companySize + '\'' +
                ", companyType='" + companyType + '\'' +
                ", campusId='" + campusId + '\'' +
                ", campusGUID='" + campusGUID + '\'' +
                '}';
    }
}
