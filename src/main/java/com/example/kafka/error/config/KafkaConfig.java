package com.example.kafka.error.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.example.kafka.error.services.kafka.data.CrashableRequest;

@EnableKafka
@Configuration
public class KafkaConfig
{
    private final String bootstrapServer;

    @Autowired
    public KafkaConfig(@Value("${kafka.bootstrap:localhost:9092}") String bootstrapServer)
    {
        this.bootstrapServer = bootstrapServer;
    }

    @Bean
    public NewTopic crashingTopic()
    {
        return TopicBuilder.name("exampleCrashingTopic")
            .partitions(1)
            .replicas(1)
            .build();
    }

    @Bean
    public ProducerFactory<String, CrashableRequest> completedReparseRequestProducerFactory()
    {
        return constructGenericJSONProducerFactory(bootstrapServer);
    }

    @Bean
    public KafkaTemplate<String, CrashableRequest> completedReparseRequestKafkaTemplate()
    {
        return new KafkaTemplate<>(completedReparseRequestProducerFactory());
    }

    private <V> ProducerFactory<String, V> constructGenericJSONProducerFactory(String bootstrapServer)
    {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public ConsumerFactory<String, CrashableRequest> consumerFactory()
    {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "exampleGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(CrashableRequest.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CrashableRequest> kafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<String, CrashableRequest> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
