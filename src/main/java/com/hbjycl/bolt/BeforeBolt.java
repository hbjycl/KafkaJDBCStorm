package com.hbjycl.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BeforeBolt extends BaseBasicBolt {


    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = (String) input.getValue(0);
        System.out.println("out=" + word);
        collector.emit(new Values(word));
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields outputFields = new Fields("user_id");

        declarer.declare(outputFields);
    }
}