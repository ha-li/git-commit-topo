package com.gecko.topology.commit.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URL;
import java.util.Map;

/**
 * Created by hlieu on 08/17/16.
 */
public class StreamCommitReader extends BaseRichSpout {
    private BufferedReader fileReader;
    private SpoutOutputCollector collector;


    public void open(Map config, TopologyContext context, SpoutOutputCollector pCollector) {
        this.collector = pCollector;

        try {
            // open file, this works when the file is in the class path
            URL path = ClassLoader.getSystemResource((String) config.get("inputFile"));
            if(null == path) {
                throw new FileNotFoundException("The input file was not found in the system resource path.");
            } else {
                this.fileReader = new BufferedReader(new FileReader(path.getFile()));
            }
        }catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        }
    }

    public void nextTuple() {
        String line;
        try {
            while ((line = this.fileReader.readLine()) != null) {
                //System.out.println(line);

                this.collector.emit(new Values(line));
            }
            //System.out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void ack(Object msgId) {}

    public void fail(Object msgId) {}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("commit-id", "email"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void activate() {}
    public void deactivate() {}

    public void close(){}

}
