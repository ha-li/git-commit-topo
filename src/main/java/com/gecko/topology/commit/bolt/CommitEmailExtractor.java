package com.gecko.topology.commit.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by hlieu on 08/20/16.
 *
 * Class is not currently being used in the topology
 */
public class CommitEmailExtractor extends BaseBasicBolt {

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("email"));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String email = tuple.getStringByField("email");
        collector.emit(new Values(email));
    }
}
