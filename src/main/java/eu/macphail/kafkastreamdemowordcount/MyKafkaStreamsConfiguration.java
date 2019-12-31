package eu.macphail.kafkastreamdemowordcount;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;

import java.util.Arrays;

@EnableKafka
@EnableKafkaStreams
@Configuration
public class MyKafkaStreamsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MyKafkaStreamsConfiguration.class);

    @Autowired
    ApplicationContext context;

    @Bean
    public StreamsBuilderFactoryBeanCustomizer customizer() {
        return fb -> fb.setStateListener((newState, oldState) -> {
            StreamsBuilderFactoryBean factory = context.getBean(StreamsBuilderFactoryBean.class);
            log.info("Factory: " + factory);
            KafkaStreams streams = factory.getKafkaStreams();
            log.info("Streams: " + streams);
            log.info("State transition from " + oldState + " to " + newState);
            if(newState == KafkaStreams.State.RUNNING) {
                ReadOnlyKeyValueStore<String, Long> store = streams.store("CountsKeyValueStore", QueryableStoreTypes.<String, Long>keyValueStore());
                log.info("Store: " + store);

                ConfigurableApplicationContext configurableApplicationContext = (ConfigurableApplicationContext) context;
                DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) configurableApplicationContext.getBeanFactory();
                beanFactory.registerSingleton("CountsKeyValueStore", store);
                log.info("Store is registered");

                WordCountResource wordCountResource = (WordCountResource) context.getBean("wordCountResource");
                wordCountResource.setKeyValueStore(store);
            }
        });
    }

    /*@Bean
    KStream<String, String> wordCountStream(StreamsBuilder builder) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> textLines = builder.stream("streams-plaintext-input", Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value)
                .count();

        wordCounts.toStream().to("streams-wordcount-output", Produced.with(stringSerde, longSerde));

        return textLines;
    }*/

    @Bean
    KStream<String, String> wordCountKTableStream(StreamsBuilder builder) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> textLines = builder.stream("streams-plaintext-input", Consumed.with(stringSerde, stringSerde));

        KGroupedStream<String, String> groupedByWord = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde));

        groupedByWord
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsKeyValueStore"));

        return textLines;
    }
}
