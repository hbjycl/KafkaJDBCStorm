package com.hbjycl.bolt;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersistentBolt {
    private static Map<String, Object> hikariConfigMap = new HashMap<String, Object>() {{
        put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        put("dataSource.url", "jdbc:mysql://192.168.3.210/test");
        put("dataSource.user", "root");
        put("dataSource.password", "123456");
    }};
    public static ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

    public static JdbcInsertBolt getJdbcInsertBolt() {
        //使用tablename进行插入数据，需要指定表中的所有字段  
        /*String tableName="userinfo"; 
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider); 
        JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper) 
                                        .withTableName("userinfo") 
                                        .withQueryTimeoutSecs(50);*/
        //使用schemaColumns，可以指定字段要插入的字段  
        List<Column> schemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR),
                new Column("resource_id", Types.VARCHAR), new Column("count", Types.INTEGER));
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(schemaColumns);
        JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into userinfo(id,user_id,resource_id,count) values(?,?,?)")
                .withQueryTimeoutSecs(50);
        return jdbcInsertBolt;
    }

    public static JdbcLookupBolt getJdbcLookupBlot() {


        Fields outputFields = new Fields("id");
        List<Column> queryParamColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        String selectSql = "select id from userinfo where user_id = ?";
        SimpleJdbcLookupMapper lookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt userNameLookupBolt = new JdbcLookupBolt(connectionProvider, selectSql, lookupMapper)
                .withQueryTimeoutSecs(30);
        return userNameLookupBolt;
    }

}