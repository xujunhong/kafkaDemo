/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.demo.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.lamfire.logger.Logger;
import com.lamfire.utils.PropertiesUtils;
import com.mob.demo.ConsumerBoot;
import kafka.message.MessageAndMetadata;

/**
 * @author zxc Jul 29, 2015 10:35:10 AM
 */
public class MessageConsumer implements Runnable {

    private static final Logger logger     = Logger.getLogger(ConsumerBoot.class);
    private static Properties   pro        = PropertiesUtils.load("kafka-consumer.properties", MessageConsumer.class);

    private String              topic;
    private int                 numThreads = 2;

    private ConsumerConfig      config;
    private ConsumerConnector   connector;

    private ExecutorService     threadPool;
    private MessageExecutor     executor;

    public MessageConsumer(String topic, int numThreads) {
        this.topic = topic;
        this.numThreads = numThreads;
        this.config = new ConsumerConfig(pro);
    }

    public MessageConsumer(String topic, int numThreads, String configFile) {
        this.topic = topic;
        this.numThreads = numThreads;
        this.config = new ConsumerConfig(PropertiesUtils.load(configFile, MessageConsumer.class));
    }

    private synchronized ConsumerConnector getConsumer() {
        if (connector == null) {
            connector = Consumer.createJavaConsumerConnector(config);
        }
        return connector;
    }

    public synchronized void close() {
        if (connector != null) {
            connector.shutdown();
            connector = null;
        }
    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, this.numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = getConsumer().createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        threadPool = Executors.newFixedThreadPool(this.numThreads);

        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            threadPool.submit(new ConsumerTest(stream, executor, threadNumber));
            threadNumber++;
        }
        logger.error("[threadPool thread num] count=" + threadNumber);
    }

    static class ConsumerTest implements Runnable {

        private int                         thNum;

        private KafkaStream<byte[], byte[]> stream;

        private MessageExecutor             executor;

        public ConsumerTest(KafkaStream<byte[], byte[]> stream, MessageExecutor executor, int thNum) {
            this.stream = stream;
            this.executor = executor;
            this.thNum = thNum;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> next = it.next();
                int partition = next.partition();
                long offset = next.offset();
                executor.execute("[CONSUMER 第" + thNum + "个线程,partition="+partition+";offset="+offset+"]", new String(it.next().message()));
            }
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    public MessageExecutor getExecutor() {
        return executor;
    }

    public void setExecutor(MessageExecutor executor) {
        this.executor = executor;
    }
}
