package com.pariveda.kafka.common;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StatsDClientFactory {
    private static final Logger log = LoggerFactory.getLogger(StatsDClientFactory.class);
    private final ConfigurationWrapper config;

    public StatsDClientFactory(ConfigurationWrapper config) {
        this.config = config;
    }

    public StatsDClient getStatsDClient() {
        Properties props = config.getPropertiesFromNamespace("statsd");

        String prefix = props.getProperty("prefix");
        String host = props.getProperty("host");
        String port = props.getProperty("port");

        log.info("Creating statsd client with (prefix, host, port): ({}, {}, {})", prefix, host, port);

        return new NonBlockingStatsDClient(prefix, host, Integer.parseInt(port));
    }
}
