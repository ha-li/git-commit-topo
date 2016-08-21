package com.gecko.topology.commit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.gecko.topology.commit.bolt.EmailCounter;
import com.gecko.topology.commit.spout.StreamCommitReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hlieu on 08/17/16.
 */
public class GitCommitTopology {


    private static int FIVE_SECONDS = 1000 * 5;
    private static int SLEEP_CYCLE = FIVE_SECONDS;

    private static String TOPOLOGY_NAME = "Git-Comit-Topology";
    /**
     * Apache Storm Topology class to count the commits to git.
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {

        Config config = new Config();
        config.put("inputFile", "commits.txt");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("commits-reader", new StreamCommitReader());
        builder.setBolt("commit-counter", new EmailCounter()).shuffleGrouping("commits-reader");
        // define the topology here

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, topology);
        Utils.sleep(SLEEP_CYCLE);

        cluster.killTopology(TOPOLOGY_NAME);
        // if you forget this, it will keep running forever
        cluster.shutdown();
    }
}
