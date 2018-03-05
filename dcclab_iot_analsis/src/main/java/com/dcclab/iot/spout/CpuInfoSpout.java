package com.dcclab.iot.spout;

import java.io.IOException;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class CpuInfoSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = LogManager.getLogger(CpuInfoSpout.class);
	
	/**
	 * Time in milliseconds to wait for a message from the queue if there is no
	 * message ready when the topology requests a tuple (via {@link #nextTuple()}).
	 */
	public static final long WAIT_FOR_NEXT_MESSAGE = 1L;

	/**
	 * Time in milliseconds to wait after losing connection to the AMQP broker
	 * before attempting to reconnect.
	 */
	public static final long WAIT_AFTER_SHUTDOWN_SIGNAL = 10000L;

	private static Connection amqpConnection;
	private static Channel amqpChannel;
	private static QueueingConsumer amqpConsumer;

	private SpoutOutputCollector collector;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			logger.info("CpuInfoSpout Open!!");
			this.collector = collector;
			setupAMQP();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {

		if (amqpConsumer != null) {
			try {
				amqpChannel.basicConsume("dcclab", false, amqpConsumer);
				final QueueingConsumer.Delivery delivery = amqpConsumer.nextDelivery();
				if (delivery == null)
					return;
				String message = new String(delivery.getBody());				
				collector.emit(new Values(message));
				logger.info("message here : "+message);
				logger.debug("message here : "+message);
				/*
				 * TODO what to do about malformed messages? Skip? Avoid infinite retry! Maybe
				 * we should output them on a separate stream.
				 */
			} catch (ShutdownSignalException e) {
				Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);

			} catch (InterruptedException e) {
				// interrupted while waiting for message, big deal
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("spoutvalue"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	private Connection getConnection() throws IOException {
		ConnectionFactory factory = new ConnectionFactory();

		amqpConnection = factory.newConnection();
		return amqpConnection;
	}

	private void setupAMQP() throws IOException {
		logger.info("setupAMQP Start!!");
		final ConnectionFactory connectionFactory = new ConnectionFactory();

		connectionFactory.setHost("cat.rmq.cloudamqp.com");
		connectionFactory.setPort(5672);
		connectionFactory.setUsername("yrvokeih");
		connectionFactory.setPassword("KuTgKr-HevhZpvcKV2c6p2sQi73aSUOR");
		connectionFactory.setVirtualHost("yrvokeih");

		this.amqpConnection = connectionFactory.newConnection();
		this.amqpChannel = amqpConnection.createChannel();

		this.amqpConsumer = new QueueingConsumer(amqpChannel);
		
	}

	private Channel getChannel() {
		int count = 3;
		while (count-- > 0) {
			try {
				if (amqpConnection == null) {
					amqpConnection = getConnection();
				}
				return amqpConnection.createChannel();
			} catch (Exception e) {

				if (amqpConnection != null) {
					try {
						amqpConnection.close();
					} catch (Exception e1) {
					}
				}
				amqpConnection = null;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e2) {
				}
			}
		}
		return null;
	}

}
