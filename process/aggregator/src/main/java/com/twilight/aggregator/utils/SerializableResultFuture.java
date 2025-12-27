package com.twilight.aggregator.utils;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import java.io.Serializable;
import java.util.Collection;
import java.util.function.Consumer;

public class SerializableResultFuture<T> implements ResultFuture<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private transient Collection<T> result;
    private final transient Consumer<Collection<T>> completeHandler;
    private final transient Consumer<Throwable> errorHandler;

    public SerializableResultFuture(Collection<T> result,
            Consumer<Collection<T>> completeHandler,
            Consumer<Throwable> errorHandler) {
        this.result = result;
        this.completeHandler = completeHandler;
        this.errorHandler = errorHandler;
    }

    @Override
    public void complete(Collection<T> result) {
        this.result = result;
        if (completeHandler != null) {
            completeHandler.accept(result);
        }
    }

    @Override
    public void completeExceptionally(Throwable error) {
        if (errorHandler != null) {
            errorHandler.accept(error);
        }
    }

    public Collection<T> getResult() {
        return result;
    }
}