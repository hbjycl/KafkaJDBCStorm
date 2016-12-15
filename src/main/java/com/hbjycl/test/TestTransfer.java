package com.hbjycl.test;

import com.hbjycl.module.DeviceInfo;
import com.hbjycl.util.TransferUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by Admin on 2016/10/25.
 */
public class TestTransfer {
    public static void main(String[] args){
        String body="00250734058850670200B08400000D016D23AA0682CDB80000005A00040207100C070D1726"; // 现在的任务id
        DeviceInfo deviceInfo = TransferUtil.tranferRequestCode(body);
        System.out.println(deviceInfo);
    }
}
