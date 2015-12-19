/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.demo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.lamfire.logger.Logger;
import com.lamfire.utils.RandomUtils;
import com.mob.demo.utils.ProducerUtil;

/**
 * @author zxc Sep 15, 2015 12:14:38 PM
 */
public class ProducerBoot {

    private static final Logger     logger                   = Logger.getLogger(ProducerBoot.class);

    public static final String      FULL_DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    // static ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);
    //
    // public static void main(String[] args) {
    // executor.scheduleAtFixedRate(new ProducerTest("thread-1"), 1, 3, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-2"), 1, 1, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-3"), 1, 2, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-4"), 1, 2, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-5"), 1, 2, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-6"), 1, 2, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-7"), 1, 1, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-8"), 1, 1, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-9"), 1, 1, TimeUnit.SECONDS);
    // executor.scheduleAtFixedRate(new ProducerTest("thread-10"), 1, 1, TimeUnit.SECONDS);
    // }

    static ExecutorService      executor                 = Executors.newScheduledThreadPool(10);
    
    public static void main(String[] args) {
        executor.submit(new ProducerTest("thread-1"));
        // executor.submit(new ProducerTest("thread-2"));
        // executor.submit(new ProducerTest("thread-3"));
        // executor.submit(new ProducerTest("thread-4"));
        // executor.submit(new ProducerTest("thread-5"));
        // executor.submit(new ProducerTest("thread-6"));
        // executor.submit(new ProducerTest("thread-7"));
        // executor.submit(new ProducerTest("thread-8"));
        // executor.submit(new ProducerTest("thread-9"));
        // executor.submit(new ProducerTest("thread-10"));
    }

    static class ProducerTest implements Runnable {

        private String message;

        public ProducerTest(String message) {
            this.message = message;
        }

        public void run() {
            long length = 10;
            for (int i = 0; i < length; i++) {
                String uuid = "my_key_" + RandomUtils.nextInt(10);
                String msg = "[PRODUCER now=" + new SimpleDateFormat(FULL_DATE_FORMAT_PATTERN).format(new Date())
                             + "] " + message + " test producer! id=" + uuid;
                ProducerUtil.send("my_topic_error_", uuid, msg);
                logger.error(msg);
            }
        }
    }
}
