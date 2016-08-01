package com.hbjycl.restapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

/**
 * Created by Admin on 2016/8/1.
 */
public class AppPush {
    private static AppPush ourInstance = new AppPush();

    public static AppPush getInstance() {
        return ourInstance;
    }

    private AppPush() {

    }

    public void push(){
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

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
