package com.dcclab.iot.bolt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CpuInfoBolt extends BaseBasicBolt {
	//Logger logger = LoggerFactory.getLogger(CpuInfoSpout.class);
	private static final Logger logger = LogManager.getLogger(CpuInfoBolt.class);
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
		//String value = input.getStringByField("message");
		String value = input.getString(0);
		collector.emit(new Values(value));
		logger.info("info value : "+value);
		logger.debug("debug value : "+value);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("boltvalue"));
	}
	
}
