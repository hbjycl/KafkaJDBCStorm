package com.hbjycl.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * Created by Admin on 2016/7/27.
 */
public class LogStoreMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;

    public LogStoreMapper(){
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.LIST, null);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("key");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getStringByField("content");
    }
}
