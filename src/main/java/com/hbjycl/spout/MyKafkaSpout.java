package com.hbjycl.spout;

import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.UUID;

/**
 * Created by Admin on 2016/11/28.
 */
public class MyKafkaSpout {

    public static KafkaSpout getKafkaSpout(String topic){
        BrokerHosts hosts = new ZkHosts("h1:2181,h2:2181,h3:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/storm", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return new KafkaSpout(spoutConfig);
    }
}
