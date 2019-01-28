package com.cpqd.app.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import com.cpqd.app.config.Config;

import java.util.Properties;

/**
 * KafkaProducer wrapper.
 */
public class Producer {
    Logger mLogger = Logger.getLogger(Producer.class);
    private static final String DATA_BROKER_DEFAULT = "http://" + Config.getInstance().getDataBrokerAddress();

    private String mBrokerManager;
    private KafkaProducer<String, String> mProducer;

    /**
     * Initializes producer instance.
     */
    public Producer() {
        this.mBrokerManager =  DATA_BROKER_DEFAULT;

        // create instance for properties to access producer configs
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getInstance().getKafkaAddress());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        mProducer = new KafkaProducer<>(props);
    }

    /**
     * Publishes a message on kafka given a topic.
     *
     * @param topic Topic to publish.
     * @param message Message to publish on the topic.
     */
    public void produce(String topic, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
        this.mProducer.send(producerRecord, new ProducerSendCallback());

    }


    private class ProducerSendCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                mLogger.error("Error producing to topic " + recordMetadata.topic());
                e.printStackTrace();
            } else {
                mLogger.debug("Producer send OK to topic " + recordMetadata.topic());
            }
        }
    }
}