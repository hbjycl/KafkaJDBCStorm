package com.hbjycl.bolt;

import com.hbjycl.restapi.AppPush;
import org.apache.log4j.Logger;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Map;

public class BeforeBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private static Logger logger = Logger.getLogger(BeforeBolt.class);

    @Override
    public void execute(Tuple tuple) {

        //app推送
        //AppPush.getInstance().push();

        String content = tuple.getStringByField("str");
        logger.info(content);

        outputCollector.emit(new Values("log", content));
        outputCollector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields outputFields = new Fields("key", "content");
        declarer.declare(outputFields);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }



}