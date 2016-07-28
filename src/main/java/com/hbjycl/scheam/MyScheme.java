package com.hbjycl.scheam;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Created by Admin on 2016/7/27.
 */
public class MyScheme implements Scheme{
    private static final Charset UTF8_CHARSET;
    private String[] names = {"str"};

    public MyScheme(String[] names) {
        this.names = names;
    }

    public List<Object> deserialize(ByteBuffer bytes) {
        return new Values(new Object[]{deserializeString(bytes)});
    }

    public static String deserializeString(ByteBuffer string) {
        if(string.hasArray()) {
            int base = string.arrayOffset();
            return new String(string.array(), base + string.position(), string.remaining());
        } else {
            return new String(Utils.toByteArray(string), UTF8_CHARSET);
        }
    }

    public Fields getOutputFields() {
        return new Fields(names);
    }

    static {
        UTF8_CHARSET = StandardCharsets.UTF_8;
    }
}
