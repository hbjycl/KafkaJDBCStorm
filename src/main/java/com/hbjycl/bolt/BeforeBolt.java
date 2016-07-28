package com.hbjycl.bolt;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.simple.SimpleLogger;
import org.apache.logging.slf4j.Log4jLogger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class BeforeBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields outputFields = new Fields("key","content");
        declarer.declare(outputFields);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String content = tuple.getStringByField("str");
        outputCollector.emit(new Values("log",content));
        outputCollector.ack(tuple);
    }


}