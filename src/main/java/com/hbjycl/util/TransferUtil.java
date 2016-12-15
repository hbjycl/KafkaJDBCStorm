package com.hbjycl.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hbjycl.module.DeviceInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.util.Objects;

/**
 * Created by Admin on 2016/12/5.
 */
public class TransferUtil {
    public static DeviceInfo tranferRequestCode(String body){
        DeviceInfo deviceInfo = new DeviceInfo();
        if(null!=body&& !StringUtils.equals("", body) &&body.length()>28) {
            deviceInfo.setLength(body.substring(0, 4));
            deviceInfo.setCusID(body.substring(4, 6));
            deviceInfo.setCardID(body.substring(6, 16));
            deviceInfo.setVersion(body.substring(16, 18));
            deviceInfo.setCommandCode(body.substring(18, 22));
            deviceInfo.setCommandId(body.substring(22, 24));
            deviceInfo.setReserved(body.substring(24, 28));
            deviceInfo.setContent(body.substring(28));

            switch (deviceInfo.getCommandId()){
                case "81":
                    deviceInfo.setLongitude(body.substring(28, 36));
                    deviceInfo.setLatitude(body.substring(36, 44));
                    deviceInfo.setSpeed(body.substring(44, 46));
                    deviceInfo.setDirection(body.substring(46, 48));
                    deviceInfo.setHeight(body.substring(48, 52));
                    deviceInfo.setStatusBits(body.substring(52, 60));
                    deviceInfo.setDate(body.substring(60, 66));
                    deviceInfo.setTime(body.substring(66, 72));
                    break;
                case "84":
                    deviceInfo.setWarnType(body.substring(28, 30));
                    deviceInfo.setLongitude(body.substring(30, 38));
                    deviceInfo.setLatitude(body.substring(38, 46));
                    deviceInfo.setSpeed(body.substring(46, 48));
                    deviceInfo.setDirection(body.substring(48, 50));
                    deviceInfo.setHeight(body.substring(50, 54));
                    deviceInfo.setStatusBits(body.substring(54, 62));
                    deviceInfo.setDate(body.substring(62, 68));
                    deviceInfo.setTime(body.substring(68, 74));
                    break;
                case "85":
                    deviceInfo.setLongitude(body.substring(28, 36));
                    deviceInfo.setLatitude(body.substring(36, 44));
                    deviceInfo.setSpeed(body.substring(44, 46));
                    deviceInfo.setDirection(body.substring(46, 48));
                    deviceInfo.setHeight(body.substring(48, 52));
                    deviceInfo.setStatusBits(body.substring(52, 60));
                    deviceInfo.setDate(body.substring(60, 66));
                    deviceInfo.setTime(body.substring(66, 72));
                    String params = body.substring(72);
                    while (params.length() > 10) {
                        int length = Integer.parseInt(params.substring(4, 8), 16);
                        if (StringUtils.equals("0001", params.substring(0, 4))) {
                            deviceInfo.setGsm(params.substring(0, 4));
                        } else if (StringUtils.equals("0002", params.substring(0, 4))) {
                            deviceInfo.setGps(params.substring(8, 8 + length * 2));
                        } else if (StringUtils.equals("2201", params.substring(0, 4))) {
                            //ECU数据
                            //ECU数据
                            params = params.substring(8);
                            while (params.length() > 24) {
                                String ecuId = params.substring(0, 8);
                                String ecuValue = params.substring(8, 24);
                                if (StringUtils.equals("18FEE000", ecuId)) {
                                    deviceInfo.setRunMile(ecuValue);
                                } else if (StringUtils.equals("18FEE500", ecuId)) {
                                    deviceInfo.setRunTime(ecuValue);
                                } else if (StringUtils.equals("18FEF100", ecuId)) {
                                    deviceInfo.setRunSpeed(ecuValue);
                                } else if (StringUtils.equals("0CF00400", ecuId)) {
                                    deviceInfo.setPower(ecuValue);
                                } else if (StringUtils.equals("18FEF200", ecuId)) {
                                    deviceInfo.setFuel(ecuValue);
                                } else if (StringUtils.equals("18FEE900", ecuId)) {
                                    deviceInfo.setTotalFuel(ecuValue);
                                } else if (StringUtils.equals("18FECA00", ecuId)) {
                                    deviceInfo.setMalfunction(ecuValue);
                                }
                                params = params.substring(24);
                            }
                            break;
                        }
                        params = params.substring(8 + length * 2);
                    }
                    break;
                case "86":
                    deviceInfo.setLongitude(body.substring(28, 36));
                    deviceInfo.setLatitude(body.substring(36, 44));
                    deviceInfo.setSpeed(body.substring(44, 46));
                    deviceInfo.setDirection(body.substring(46, 48));
                    deviceInfo.setHeight(body.substring(48, 52));
                    deviceInfo.setStatusBits(body.substring(52, 60));
                    deviceInfo.setDate(body.substring(60, 66));
                    deviceInfo.setTime(body.substring(66, 72));
                    deviceInfo.setOnOff(body.substring(72, 74));
            }
        }


        RestTemplate restTemplate = new RestTemplate();
        try {

            if (StringUtils.isNotEmpty(deviceInfo.getLongitude()) && StringUtils.isNotEmpty(deviceInfo.getLatitude())) {
                String result = restTemplate.getForObject("http://api.zdoz.net/transgps.aspx?lat=" + Double.parseDouble(String.valueOf(Integer.parseInt(deviceInfo.getLongitude(), 16))) / 1000000 + "&lng=" + Double.parseDouble(String.valueOf(Integer.parseInt(deviceInfo.getLatitude(), 16))) / 1000000, String.class);
                JSONObject jsonObject = JSON.parseObject(result);
                deviceInfo.setLongitude(jsonObject.get("Lng").toString());
                deviceInfo.setLatitude(jsonObject.get("Lat").toString());
            }

            if (StringUtils.isNotEmpty(deviceInfo.getLatitude())) {
                deviceInfo.setSpeed(Integer.parseInt(deviceInfo.getSpeed(), 16) + "");
            }
            if (StringUtils.isNotEmpty(deviceInfo.getLatitude())) {
                deviceInfo.setDate("20" + String.format("%02d", Integer.parseInt(deviceInfo.getDate().substring(0, 2), 16)) + "-" +
                        String.format("%02d", Integer.parseInt(deviceInfo.getDate().substring(2, 4), 16)) + "-" +
                        String.format("%02d", Integer.parseInt(deviceInfo.getDate().substring(4, 6), 16)));
            }
            if (StringUtils.isNotEmpty(deviceInfo.getLatitude())) {
                deviceInfo.setTime(String.format("%02d", Integer.parseInt(deviceInfo.getTime().substring(0, 2), 16)) + ":" +
                        String.format("%02d", Integer.parseInt(deviceInfo.getTime().substring(2, 4), 16)) + ":" +
                        String.format("%02d", Integer.parseInt(deviceInfo.getTime().substring(4, 6), 16)));
            }
        if (StringUtils.isNotEmpty(deviceInfo.getRunTime())) {
            //运转时间
            deviceInfo.setRunTime(Integer.parseInt(ecuTransfer(deviceInfo.getRunTime(),1), 16) + "");
        }
        if (StringUtils.isNotEmpty(deviceInfo.getRunMile())) {
            //累计里程
            deviceInfo.setTotalMile(Integer.parseInt(ecuTransfer(deviceInfo.getRunMile(),1), 16) + "");
            //单程里程
            deviceInfo.setRunMile(Integer.parseInt(ecuTransfer(deviceInfo.getRunMile(),0), 16) + "");
        }
/*        if(StringUtils.isNotEmpty(deviceInfo.getPower())) {
            deviceInfo.setPower(Integer.parseInt(deviceInfo.getPower(), 16) + "");
        }*/
        if(StringUtils.isNotEmpty(deviceInfo.getFuel())) {
            //实时耗油
            deviceInfo.setFuel(Integer.parseInt(ecuTransfer(deviceInfo.getFuel(),0).substring(4,8), 16) + "");
        }
        if (StringUtils.isNotEmpty(deviceInfo.getTotalFuel())) {
            //单程耗油
            //累计耗油
            deviceInfo.setTotalFuel(Integer.parseInt(ecuTransfer(deviceInfo.getTotalFuel(),1), 16) + "");
        }
/*        if(StringUtils.isNotEmpty(deviceInfo.getRunSpeed())) {
            deviceInfo.setRunSpeed(Integer.parseInt(deviceInfo.getRunSpeed(), 16) + "");
        }*/
            if (StringUtils.isNotEmpty(deviceInfo.getGsm())) {
                deviceInfo.setGsm(Integer.parseInt(deviceInfo.getGsm(), 16) + "");
            }
            if (StringUtils.isNotEmpty(deviceInfo.getGps())) {
                deviceInfo.setGps(Integer.parseInt(deviceInfo.getGps(), 16) + "");
            }
            if (StringUtils.isNotEmpty(deviceInfo.getHeight())) {
                deviceInfo.setHeight(Integer.parseInt(deviceInfo.getHeight(), 16) + "");
            }
            if (StringUtils.isNotEmpty(deviceInfo.getWarnType())) {
                String type = String.format("%08d",Integer.parseInt(Integer.toBinaryString(Integer.valueOf(deviceInfo.getWarnType(),16))));
                String type1 = type.substring(0,4);
                String type2=type.substring(4,8);
                String result=null;

                switch (type2){
                    case "0010":result = "GPS天线未接或剪断报警";break;
                    case "0011":result = "GPS模块故障报警";break;
                    case "0100":result = "低电压报警";break;
                    case "0110": result = "断电报警";break;
                    case "1000":result="断电报警解除";break;
                    case "1001":result="拆盖报警";break;
                    case "1010":result="区域报警";break;
                    case "1100":result="485模块未接或故障报警";break;
                }
                if(StringUtils.equals("1",type1.substring(0,1))){
                    result = "解除"+result;
                }
                deviceInfo.setWarnType(result);
            }
            if (StringUtils.isNotEmpty(deviceInfo.getOnOff())) {
                if("00".equals(deviceInfo.getOnOff())){
                    deviceInfo.setOnOff("开");
                }else if("01".equals(deviceInfo.getOnOff())){
                    deviceInfo.setOnOff("关");
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return deviceInfo;
    }

    private static String ecuTransfer(String data,int index){
       return data.substring(8*index+6,8*index+8)+data.substring(8*index+4,8*index+6)+data.substring(8*index+2,8*index+4)+data.substring(8*index,8*index+2);
    }

}
