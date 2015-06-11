package com.spnotes.storm;
import backtype.storm.tuple.Fields;
import com.spnotes.storm.bolts.WordCounterBolt;
import com.spnotes.storm.bolts.WordSpitterBolt;
import com.spnotes.storm.spouts.LineReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class HelloStorm {

	public static void main(String[] args) throws Exception{
		Config config = new Config();
		config.put("inputFile", args[0]);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line-reader-spout", new LineReaderSpout());
		builder.setBolt("word-spitter", new WordSpitterBolt()).shuffleGrouping("line-reader-spout").setNumTasks(10).setMaxTaskParallelism(10);
		builder.setBolt("word-counter", new WordCounterBolt()).fieldsGrouping("word-spitter", new Fields("word")).setNumTasks(10).setMaxTaskParallelism(10);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("HelloStorm", config, builder.createTopology());
		Thread.sleep(10000);
		
		cluster.shutdown();
	}

}
