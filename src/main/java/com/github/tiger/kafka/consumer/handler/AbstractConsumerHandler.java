package com.github.tiger.kafka.consumer.handler;

import com.github.tiger.kafka.consumer.ConsumerHandler;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liuhongming
 */
public abstract class AbstractConsumerHandler<R> implements ConsumerHandler<R> {

    protected R records;

    protected AtomicBoolean done = new AtomicBoolean(false);

    @Override
    public void commit(R records) throws Exception {
        this.records = records;
        done.set(false);
        try {
            handle(records);
        } finally {
            done.set(true);
        }
    }

    @Override
    public boolean isDone() {
        return done.get();
    }

}
