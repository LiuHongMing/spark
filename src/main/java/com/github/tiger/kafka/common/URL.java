package com.github.tiger.kafka.common;

import java.util.Map;

/**
 * @author liuhongming
 */
public class URL {

    private final String path;

    private final Map<String, String> parameters;

    protected URL() {
        this.path = null;
        this.parameters = null;
    }

    public URL(String path) {
        this.path = path;
        this.parameters = null;
    }

    public URL(String path, Map<String, String> parameters) {
        this.path = path;
        this.parameters = parameters;
    }

    public String getPath() {
        return path;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getParameter(String key) {
        String value = parameters.get(key);
        if (value == null || value.length() == 0) {
            value = parameters.get(key);
        }
        return value;
    }

}
