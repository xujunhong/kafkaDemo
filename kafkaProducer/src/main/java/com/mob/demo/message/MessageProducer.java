/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.demo.message;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.I0Itec.zkclient.ZkClient;

import com.lamfire.logger.Logger;
import com.lamfire.utils.PropertiesUtils;

/**
 * @author zxc Jul 29, 2015 10:35:10 AM
 */
public class MessageProducer {

    private static final Logger          LOGGER           = Logger.getLogger(MessageProducer.class);
    private static final String          default_producer = "kafka-producer.properties";

    private static final MessageProducer instance         = new MessageProducer();

    public static MessageProducer getInstance() {
        return instance;
    }

    private Properties               props;
    private ProducerConfig           config;
    private Producer<String, String> producer;

    private MessageProducer() {
        props = PropertiesUtils.load(default_producer, MessageProducer.class);
        config = new ProducerConfig(props);
    }

    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("192.168.180.73:2181,192.168.180.74:2181,192.168.180.75:2181", 10000, 10000);
        if (!AdminUtils.topicExists(zkClient, "my_error")) {
            AdminUtils.createTopic(zkClient, "my_error", 5, 3, new Properties());
        } else {

        }
    }

    public void send(String topicName, String key, String message) {
        try {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, key, message);
            getProducer().send(data);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private synchronized Producer<String, String> getProducer() {
        if (producer == null) {
            producer = new Producer<String, String>(config);
        }
        return producer;
    }

    public synchronized void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
}
