package com.gecko.topology.commit.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by hlieu on 08/20/16.
 */
public class EmailCounter implements IRichBolt {

    private Map<String, Integer> emailTracker;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("email"));
    }

    public Map<String, Object> getComponentConfiguration(){
        return null;
    }

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.emailTracker = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String email = tuple.getStringByField("email");

        Integer count = emailTracker.get(email);
        Integer newCount = (count == null ? 1 : ++count);
        emailTracker.put(email, newCount);
    }

    public void cleanup(){
        Set<String> keys = emailTracker.keySet();
        for(String key: keys) {
            System.out.println(key + " count:" + emailTracker.get(key));
        }
    }
}
