package com.pariveda.kafka.helpers;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StatsDHelper {
    private static final Logger log = LoggerFactory.getLogger(StatsDHelper.class);

    public static StatsDClient getConsumerStatsDClient() {
        Properties props = ConfigurationHelper.getConsumerProperties("statsd");
        return getStatsDClient(props);
    }

    public static StatsDClient getProducerStatsDClient() {
        Properties props = ConfigurationHelper.getProducerProperties("statsd");
        return getStatsDClient(props);
    }

    private static StatsDClient getStatsDClient(Properties props) {
        String prefix = props.getProperty("prefix");
        String host = props.getProperty("host");
        String port = props.getProperty("port");

        log.info("Creating statsd client with (prefix, host, port): ({}, {}, {})", prefix, host, port);

        return new NonBlockingStatsDClient(prefix, host, Integer.parseInt(port));
    }
}
