package com.gecko.topology.commit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.gecko.topology.commit.spout.StreamCommitReader;

/**
 * Created by hlieu on 08/18/16.
 */
public class TestMain {
    public static void main(String[] args) {

        Config config = new Config();
        config.put("inputFile", "commits.txt");

        //TopologyBuilder builder = new TopologyBuilder();
        // define the topology here

        //StormTopology topology = builder.createTopology();

        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("Git-Comit-Topology", config, topology);
        StreamCommitReader reader = new StreamCommitReader();
        reader.open(config, null, null);
        reader.nextTuple();
    }
}
