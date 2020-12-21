package com.morgan.storm.topology;

import com.morgan.storm.bolt.LogParseBolt;
import com.morgan.storm.bolt.ProductCountBolt;
import com.morgan.storm.spout.AccessLogKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * @Description 热门商品Topology
 * @Author Morgan
 * @Date 2020/12/21 15:40
 **/
public class HotProductTopology {

    public void runStorm(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("AccessLogKafkaSpout",new AccessLogKafkaSpout(),1);
        builder.setBolt("LogParseBolt", new LogParseBolt(),5).setNumTasks(5).shuffleGrouping("accessLogList");
        builder.setBolt("ProductCountBolt", new ProductCountBolt(),5).setNumTasks(10).fieldsGrouping("LogParseBolt",new Fields("productId"));

        Config config = new Config();
        if (args !=null && args.length > 0){
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        }else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HotProductTopology",config,builder.createTopology());
            Utils.sleep(5000);
            cluster.shutdown();
        }
    }
}
