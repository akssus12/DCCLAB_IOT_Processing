package com.dcclab.iot;

import java.util.Arrays;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import com.dcclab.iot.bolt.CpuInfoBolt;
import com.dcclab.iot.spout.CpuInfoSpout;

public class executeCpuInfoSensing {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		CpuInfoSpout cpuSpout = new CpuInfoSpout();
		// CpuInfoBolt cpuBolt = new CpuInfoBolt();

		Config config = new Config();
		config.setDebug(true);
		config.put(Config.NIMBUS_HOST, "163.239.22.53");
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("163.239.22.53"));
		config.put(Config.TOPOLOGY_DEBUG, true);

		builder.setSpout("CpuInfoSpout", cpuSpout, 5);
		builder.setBolt("CpuInfoBolt", new CpuInfoBolt(), 12).shuffleGrouping("CpuInfoSpout");

		StormSubmitter submitter = new StormSubmitter();
		System.setProperty("storm.jar", "YOUR JAR File Path");
	
		submitter.submitTopology("cpuInfoTest3", config, builder.createTopology());
	}

}
