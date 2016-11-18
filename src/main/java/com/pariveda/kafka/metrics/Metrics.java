package com.pariveda.kafka.metrics;

public class Metrics implements MetricsMBean {
    private String message = null;

    public Metrics() {
        message = "Hello, world";
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void sayHello() {
        System.out.println(message);
    }
}