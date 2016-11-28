package com.hbjycl.bolt;

import com.hbjycl.module.RequestCode;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Objects;

public class WufengBeforeBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private static Logger logger = Logger.getLogger(WufengBeforeBolt.class);

    @Override
    public void execute(Tuple tuple) {

        String body = tuple.getStringByField("str");
        logger.info("content is :"+body);
        RequestCode requestCode = new RequestCode();
        if(null!=body&& !Objects.equals("", body) &&body.length()>28){
            requestCode.setLength(body.substring(0, 4));
            requestCode.setCusID(body.substring(4, 6));
            requestCode.setCardID(body.substring(6, 16));
            requestCode.setVersion(body.substring(16, 18));
            requestCode.setCommandCode(body.substring(18, 22));
            requestCode.setCommandId(body.substring(22, 24));
            requestCode.setReserved(body.substring(24, 28));
            requestCode.setContent(body.substring(28));
        }

        outputCollector.emit(new Values(
                requestCode.getLength(),
                requestCode.getCusID(),
                requestCode.getCardID(),
                requestCode.getVersion(),
                requestCode.getCommandCode(),
                requestCode.getCommandId(),
                requestCode.getReserved(),
                requestCode.getCommandCode(),
                body
                ));
        outputCollector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields outputFields = new Fields("length","cus_id","card_id","version","command_code","command_id","reserved","content","body");
        declarer.declare(outputFields);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }



}