
import com.hbjycl.bolt.AfterBolt;
import com.hbjycl.bolt.BeforeBolt;
import com.hbjycl.bolt.PersistentBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StormKafkaTopo {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("h1:2181,h2:2181,h3:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "jdbctopic", "/storm", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("spout", kafkaSpout, 5);


        Properties props = new Properties();
        props.put("bootstrap.servers", "h1:9092,h2:9092,h3:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaBolt bolt = new KafkaBolt().withProducerProperties(props);

        builder.setBolt("beforebolt", new BeforeBolt(),1).shuffleGrouping("spout");

        builder.setBolt("jdbcbolt", PersistentBolt.getJdbcLookupBlot(),2).shuffleGrouping("beforebolt");
        builder.setBolt("afterbolt", new AfterBolt(),1).shuffleGrouping("jdbcbolt");


        builder.setBolt("kafkabolt", bolt,2).shuffleGrouping("afterbolt");


        Config config = new Config();
        config.put("metadata.broker.list","h1:9092,h2:9092,h3:9092");
        config.put("key.serializer.class","kafka.serializer.StringEncoder");
        config.put("topic", "topic2");

        StormSubmitter.submitTopology(UUID.randomUUID().toString(), config, builder.createTopology());


    }
}