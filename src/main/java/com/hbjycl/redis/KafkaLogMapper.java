package com.hbjycl.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * Created by Admin on 2016/7/28.
 */
public class KafkaLogMapper implements RedisStoreMapper {
    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.LIST);
    }

    @Override
    public String getKeyFromTuple(ITuple iTuple) {
        return iTuple.getStringByField("key");
    }

    @Override
    public String getValueFromTuple(ITuple iTuple) {
        return iTuple.getStringByField("content");
    }
}
