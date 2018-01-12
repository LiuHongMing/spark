package com.github.tiger.kafka.common;

/**
 * @author liuhongming
 */
public abstract class Environment {

    public Environment() {
    }

    abstract String[] getDefaultProfiles();

}
