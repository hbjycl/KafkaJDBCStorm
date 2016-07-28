
import com.hbjycl.bolt.BeforeBolt;
import com.hbjycl.bolt.PersistentBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

public class StormKafkaTopo {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("h1:2181,h2:2181,h3:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "jdbctopic", "/storm", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("spout", kafkaSpout, 5);

        builder.setBolt("beforebolt", new BeforeBolt(),1).shuffleGrouping("spout");

        builder.setBolt("jdbcbolt", PersistentBolt.getJdbcInsertBolt(),2).shuffleGrouping("beforebolt");

        Config config = new Config();

        StormSubmitter.submitTopology(UUID.randomUUID().toString(), config, builder.createTopology());


    }
}