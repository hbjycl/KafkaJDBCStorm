package com.hbjycl.bolt;

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


        //需要请求的restful地址
        try {
            URL url = new URL("https://api.jpush.cn/v3/push");
            //打开restful链接
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            // 提交模式
            conn.setRequestMethod("POST");//POST GET PUT DELETE
            // 设置访问提交模式，表单提交
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization", "Basic ODk2YTk0ZWUxZTczNDY5YWExZjU2NDZkOjhlZDg1ZTM5MzZmYzBmN2Y0NDRhMDlhZA==");
            conn.setConnectTimeout(10000);//连接超时 单位毫秒
            conn.setReadTimeout(2000);//读取超时 单位毫秒
            conn.setDoOutput(true);// 是否输入参数
            String params = "{\n" +
                    "   \"platform\": \"all\",\n" +
                    "   \"audience\" : \"all\",\n" +
                    "   \"notification\" : {\n" +
                    "       \"android\" : {\n" +
                    "             \"alert\" : \"登陆标题\", \n" +
                    "             \"title\" : \"您有新短消息，请注意查收\", \n" +
                    "             \"builder_id\" : 3, \n" +
                    "             \"extras\" : {\n" +
                    "                  \"news_id\" : 134, \n" +
                    "                  \"my_key\" : \"a value\"\n" +
                    "             }\n" +
                    "        }\n" +
                    "   }\n" +
                    "}";
            byte[] bytes = params.getBytes();
            conn.getOutputStream().write(bytes);// 输入参数
            // 读取请求返回值
            InputStream inStream = conn.getInputStream();
            inStream.read(bytes, 0, inStream.available());
            logger.info(params);

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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