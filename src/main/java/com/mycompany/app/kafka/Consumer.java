package com.mycompany.app.kafka;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import com.mycompany.app.config.Config;
import org.apache.log4j.Logger;
import org.json.zip.None;


public class Consumer implements Runnable {
    Logger mLogger = Logger.getLogger(Consumer.class);

    private KafkaConsumer<String, String> mConsumer;
    private List<String> mTopics;
    boolean shouldStop;
    BiFunction<String, String, Integer> mCallback;

    public Consumer(BiFunction<String, String, Integer> callback) {

        this.mTopics = new ArrayList<>();
        this.mCallback = callback;
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getInstance().getKafkaAddress());
        props.put("group.id", Config.getInstance().getKafkaDefaultGroupId());
        props.put("session.timeout.ms", Config.getInstance().getKafkaDefaultSessionTimeout());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.mConsumer = new KafkaConsumer<>(props);
    }



    public void subscribe(String topic){
        mTopics.add(topic);
    }

    @Override
    public void run() {
        this.shouldStop = false;
        try {
            mLogger.info("Consumer thread is started and running...");
            System.out.println("Subscribing to topics: " + this.mTopics);
            this.mConsumer.subscribe(this.mTopics, new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    mLogger.info("topic-partitions are revoked from this consumer: " + Arrays.toString(partitions.toArray()));
                }
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    mLogger.info("topic-partitions are assigned to this consumer: " + Arrays.toString(partitions.toArray()));
                }
            });

            while (!this.shouldStop) {
//                System.out.println("shouldStop: " + this.shouldStop);
                ConsumerRecords<String, String> records = mConsumer.poll(10000);
                for (ConsumerRecord<String, String> record : records) {
                    this.mCallback.apply(record.topic(), record.value());
                }
            }
            System.out.println("AKKKKKKKKKKKKKKAKAKA");
            this.run();
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            mConsumer.close();
        }
    }

    public void shutdown() {
        mConsumer.wakeup();
    }

    public synchronized void setShouldStop(boolean set){
        this.shouldStop = set;
    }
}