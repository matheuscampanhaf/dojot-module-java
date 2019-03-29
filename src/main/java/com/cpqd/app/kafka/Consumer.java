package com.cpqd.app.kafka;

import java.util.*;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import com.cpqd.app.config.Config;

/**
 * KafkaConsumer wrapper
 */
public class Consumer implements Runnable {

    private KafkaConsumer<String, String> mConsumer;
    private List<String> mTopics;
    boolean shouldStop;
    BiFunction<String, String, Integer> mCallback;

    /**
     * Create a consumer instance
     *
     * @param callback Function to be called when receive a kafka message.
     */
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


    /**
     * Adds topic to subscribe.
     *
     * @param topic topic to subscribe.
     */
    public void subscribe(String topic){
        mTopics.add(topic);
    }

    /**
     * Initiate the consumer thread.
     */
    @Override
    public void run() {
        this.shouldStop = false;
        try {
            while (true) {
                System.out.println("Subscribing to topics: " + this.mTopics);
                this.mConsumer.subscribe(this.mTopics, new ConsumerRebalanceListener() {
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    }
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    }
                });

                while (!this.shouldStop) {
    //                System.out.println("shouldStop: " + this.shouldStop);
                    ConsumerRecords<String, String> records = mConsumer.poll(10000);
                    for (ConsumerRecord<String, String> record : records) {
                        this.mCallback.apply(record.topic(), record.value());
                    }
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
            // This thread will only stop if this exception is thrown (which
            // is done by calling 'shutdown')
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