package com.gecko.topology.commit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.gecko.topology.commit.spout.StreamCommitReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hlieu on 08/17/16.
 */
public class GitCommitTopology {


    /**
     * Apache Storm Topology class to count the commits to git.
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {

        Config config = new Config();
        config.put("inputFile", "commits.txt");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("commits-reader", new StreamCommitReader());
        // define the topology here

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Git-Comit-Topology", config, topology);
        Thread.sleep(1000);

        // if you forget this, it will keep running forever
        cluster.shutdown();
    }
}
