package com.pariveda.kafka.common;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

public class ConfigurationWrapper {
    private static final Logger log = LoggerFactory.getLogger(ConfigurationWrapper.class);
    private final Configuration config;

    public ConfigurationWrapper(String propertiesFile) throws ConfigurationException {
        Configurations configs = new Configurations();
        config = configs.properties(new File(propertiesFile));
    }

    public Properties getPropertiesFromNamespace(String propertiesNamespace) {
        Properties props = new Properties();
        Iterator<String> namespaceKeys = config.getKeys(propertiesNamespace);

        while (namespaceKeys.hasNext()) {
            String fullKey = namespaceKeys.next();
            String propsKey = fullKey.substring(fullKey.indexOf(propertiesNamespace) + propertiesNamespace.length() + 1);

            props.put(propsKey, config.getString(fullKey));
        }

        return props;
    }
}
