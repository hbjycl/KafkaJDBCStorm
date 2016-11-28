
import com.hbjycl.bolt.WufengBeforeBolt;
import com.hbjycl.bolt.WufengJdbcBolt;
import com.hbjycl.spout.MyKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class StormKafkaTopo {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("wufengSpout", MyKafkaSpout.getKafkaSpout("wufeng1"), 2);

        builder.setBolt("wufengBolt", new WufengBeforeBolt(),2).shuffleGrouping("wufengSpout");


/*        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().
                setHost(MyProperties.REDIS_HOST).setPassword(MyProperties.REDIS_PASS).setTimeout(1000000).build();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig,new KafkaLogMapper());
        builder.setBolt("redisbolt",storeBolt,2).shuffleGrouping("beforeBolt");*/

        builder.setBolt("wufengJdbcBolt", WufengJdbcBolt.getJdbcInsertBolt(),2).shuffleGrouping("wufengBolt");

        Config config = new Config();

        StormSubmitter.submitTopology("wufengJdbcTopo", config, builder.createTopology());


    }
}