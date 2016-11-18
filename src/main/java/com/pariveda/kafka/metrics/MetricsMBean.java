package com.pariveda.kafka.metrics;

public interface MetricsMBean {
    void setMessage(String message);
    String getMessage();
    void sayHello();
}