package com.pariveda.kafka.helpers;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

public class ConfigurationHelper {
    private static final String PRODUCER_PROPERTIES = "producer.properties";
    private static final String CONSUMER_PROPERTIES = "consumer.properties";

    public static Properties getProducerProperties(String prefix) {
        return getProperties(PRODUCER_PROPERTIES, prefix);
    }

    public static Properties getConsumerProperties(String prefix) {
        return getProperties(CONSUMER_PROPERTIES, prefix);
    }

    private static Properties getProperties(String propertiesFile, String prefix) {
        Configurations configs = new Configurations();
        Properties props = new Properties();

        try
        {
            Configuration config = configs.properties(new File(propertiesFile));
            Iterator<String> it = config.getKeys(prefix);

            while (it.hasNext()) {
                String key = it.next();
                props.put(key.substring(key.indexOf('.')+1), config.getString(key));
            }
        }
        catch (ConfigurationException cex)
        {
            System.out.println("configuration error: " + cex.getMessage());
        }

        return props;
    }
}
