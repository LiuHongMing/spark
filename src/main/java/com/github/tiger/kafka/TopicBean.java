package com.github.tiger.kafka;

/**
 * @author liuhongming
 */
public class TopicBean {

    private String exchangName;

    private String forwardingUrl;

    private String forwardingUrlParams;

    public String getExchangName() {
        return exchangName;
    }

    public void setExchangName(String exchangName) {
        this.exchangName = exchangName;
    }

    public String getForwardingUrl() {
        return forwardingUrl;
    }

    public void setForwardingUrl(String forwardingUrl) {
        this.forwardingUrl = forwardingUrl;
    }

    public String getForwardingUrlParams() {
        return forwardingUrlParams;
    }

    public void setForwardingUrlParams(String forwardingUrlParams) {
        this.forwardingUrlParams = forwardingUrlParams;
    }

    @Override
    public String toString() {
        return "BizBean{" +
                "exchangName='" + exchangName + '\'' +
                ", forwardingUrl='" + forwardingUrl + '\'' +
                ", forwardingUrlParams='" + forwardingUrlParams + '\'' +
                '}';
    }
}
