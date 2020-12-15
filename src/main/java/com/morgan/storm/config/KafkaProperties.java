package com.morgan.storm.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @Description Kafka配置参数
 * @Author Morgan
 * @Date 2020/12/15 12:14
 **/
@Component
@Data
public class KafkaProperties {

    @Value("spring.kafka.bootstrap-servers")
    private String Servers;
    @Value("spring.kafka.consumer.group-id")
    private String groupId;
    @Value("spring.kafka.consumer.auto-offset-reset")
    private String autoOffsetReset;
    @Value("spring.kafka.consumer.enable-auto-commit")
    private String enableAutoCommit;
    @Value("spring.kafka.consumer.key-deserializer")
    private String keyDeserializer;
    @Value("spring.kafka.consumer.value-deserializer")
    private String valueDeserializer;
}
