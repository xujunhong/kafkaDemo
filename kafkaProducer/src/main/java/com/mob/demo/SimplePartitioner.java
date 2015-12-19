/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.demo;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import com.lamfire.logger.Logger;

/**
 * @author zxc Sep 15, 2015 12:27:13 PM
 */
public class SimplePartitioner implements Partitioner {

    private static final Logger logger = Logger.getLogger(ProducerBoot.class);

    public SimplePartitioner(VerifiableProperties props) {

    }

    public int partition(Object key, int numPartitions) {
        int partition = 0;
        if (key == null) {
            Random random = new Random();
            logger.error("key is null ");
            return random.nextInt(numPartitions);
        } else {
            partition = Math.abs(key.hashCode()) % numPartitions;
            logger.error("key is " + key + " partitions is " + partition);
            return partition;
        }
    }
}
