package com.hbjycl.bolt;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WufengJdbcBolt {
    private static Map<String, Object> hikariConfigMap = new HashMap<String, Object>() {{
        put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        put("dataSource.url", "jdbc:mysql://192.168.3.221:3307/wufeng");
        put("dataSource.user", "wufeng");
        put("dataSource.password", "123456");
    }};
    public static ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

    public static JdbcInsertBolt getJdbcInsertBolt() {
        List<Column> schemaColumns = Lists.newArrayList(
                new Column("length", Types.VARCHAR),
                new Column("cus_id", Types.VARCHAR),
                new Column("card_id", Types.VARCHAR),
                new Column("version", Types.VARCHAR),
                new Column("command_code", Types.VARCHAR),
                new Column("command_id", Types.VARCHAR),
                new Column("reserved", Types.VARCHAR),
                new Column("longitude", Types.VARCHAR),
                new Column("latitude", Types.VARCHAR),
                new Column("speed", Types.VARCHAR),
                new Column("direction", Types.VARCHAR),
                new Column("height", Types.VARCHAR),
                new Column("status_bits", Types.VARCHAR),
                new Column("date", Types.VARCHAR),
                new Column("time", Types.VARCHAR),
                new Column("gsm", Types.VARCHAR),
                new Column("gps", Types.VARCHAR),
                new Column("run_time", Types.VARCHAR),
                new Column("run_mile", Types.VARCHAR),
                new Column("total_mile", Types.VARCHAR),
                new Column("run_speed", Types.VARCHAR),
                new Column("power", Types.VARCHAR),
                new Column("fuel", Types.VARCHAR),
                new Column("total_fuel", Types.VARCHAR),
                new Column("malfunction", Types.VARCHAR) ,
                new Column("warn_type", Types.VARCHAR),
                new Column("on_off", Types.VARCHAR),
                new Column("content", Types.VARCHAR),
                new Column("body", Types.VARCHAR)
                );
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(schemaColumns);
        JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into device_info(" +
                        "length,"+
                        "cus_id,"+
                        "card_id,"+
                        "version,"+
                        "command_code,"+
                        "command_id,"+
                        "reserved,"+
                        "longitude,"+
                        "latitude,"+
                        "speed,"+
                        "direction,"+
                        "height,"+
                        "status_bits,"+
                        "date,"+
                        "time,"+
                        "gsm,"+
                        "gps,"+
                        "run_time,"+
                        "run_mile,"+
                        "total_mile,"+
                        "run_speed,"+
                        "power,"+
                        "fuel,"+
                        "total_fuel,"+
                        "malfunction,"+
                        "warn_type,"+
                        "on_off,"+
                        "content,"+
                        "body"+
                ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .withQueryTimeoutSecs(50);
        return jdbcInsertBolt;
    }


}