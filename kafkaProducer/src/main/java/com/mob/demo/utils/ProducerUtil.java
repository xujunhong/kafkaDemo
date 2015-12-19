/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.demo.utils;

import com.lamfire.logger.Logger;
import com.mob.demo.message.MessageProducer;

/**
 * @author zxc Jul 29, 2015 10:35:10 AM
 */
public class ProducerUtil {

    private static final Logger LOGGER               = Logger.getLogger(MessageProducer.class);

    /**
     * 通用的kafka生产者
     * 
     * @param topic topic name
     * @param key 用于分区的key
     * @param message 消息内容
     */
    public static void send(String topic, String key, String message) {
        MessageProducer producer = null;
        try {
            producer = MessageProducer.getInstance();
            producer.send(topic, key, message);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            // producer.close();
        }
    }
}
