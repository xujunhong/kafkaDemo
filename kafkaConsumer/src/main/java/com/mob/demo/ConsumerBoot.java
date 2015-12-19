/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.demo;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.lamfire.logger.Logger;
import com.lamfire.utils.RandomUtils;
import com.mob.demo.utils.*;

/**
 * @author zxc Sep 15, 2015 12:14:59 PM
 */
public class ConsumerBoot {

    private static final Logger logger                   = Logger.getLogger(ConsumerBoot.class);
    public static final String  FULL_DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) {
        MessageConsumer consumer = new MessageConsumer("my_topic_error_", 2);
        consumer.setExecutor(new MessageExecutor() {

            @Override
            public void execute(String tips, String message) {
                logger.error(tips + " ; " + message);
                String uuid = "my_key_" + RandomUtils.nextLong();
                String msg = "[PRODUCER-CONSUMER now="
                             + new SimpleDateFormat(FULL_DATE_FORMAT_PATTERN).format(new Date()) + "] " + message
                             + " test producer! id=" + uuid;
                ProducerUtil.send("my_topic_error_", uuid, msg);
            }
        });
        consumer.run();
    }
}
