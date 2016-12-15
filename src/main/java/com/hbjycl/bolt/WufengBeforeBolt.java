package com.hbjycl.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hbjycl.module.DeviceInfo;
import com.hbjycl.util.TransferUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class WufengBeforeBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private static Logger logger = Logger.getLogger(WufengBeforeBolt.class);

    @Override
    public void execute(Tuple tuple) {

        String body = tuple.getStringByField("str");
        logger.info("content is :"+body);
            //数据解析
          DeviceInfo  deviceInfo = TransferUtil.tranferRequestCode(body);
        outputCollector.emit(new Values(
                deviceInfo.getLength(),
                deviceInfo.getCusID(),
                deviceInfo.getCardID(),
                deviceInfo.getVersion(),
                deviceInfo.getCommandCode(),
                deviceInfo.getCommandId(),
                deviceInfo.getReserved(),
                deviceInfo.getLongitude(),
                deviceInfo.getLatitude(),
                deviceInfo.getSpeed(),
                deviceInfo.getDirection(),
                deviceInfo.getHeight(),
                deviceInfo.getStatusBits(),
                deviceInfo.getDate(),
                deviceInfo.getTime(),
                deviceInfo.getGsm(),
                deviceInfo.getGps(),
                deviceInfo.getRunTime(),
                deviceInfo.getRunMile(),
                deviceInfo.getTotalMile(),
                deviceInfo.getRunSpeed(),
                deviceInfo.getPower(),
                deviceInfo.getFuel(),
                deviceInfo.getTotalFuel(),
                deviceInfo.getMalfunction(),
                deviceInfo.getWarnType(),
                deviceInfo.getOnOff(),
                deviceInfo.getContent(),
                body
                ));
        outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields outputFields = new Fields(
                "length",
                "cus_id",
                "card_id",
                "version",
                "command_code",
                "command_id",
                "reserved",
                "longitude",
                "latitude",
                "speed",
                "direction",
                "height",
                "status_bits",
                "date",
                "time",
                "gsm",
                "gps",
                "run_time",
                "run_mile",
                "total_mile",
                "run_speed",
                "power",
                "fuel",
                "total_fuel",
                "malfunction",
                "warn_type",
                "on_off",
                "content",
                "body");
        declarer.declare(outputFields);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }



}