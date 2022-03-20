package com.ozeanly.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;

/**
 * Common configuration for the Kafka cluster to be use in Producers,
 * Consumers and Admin
 */
public abstract class KafkaClusterConfig {

    private String bootstrapServers;

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }


    /**
     * Creates the common configuration properties
     * @return
     */
    protected Map<String, Object> commonConfigurations() {
        final Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }
}
