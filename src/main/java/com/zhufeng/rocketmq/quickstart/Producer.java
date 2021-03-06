package com.zhufeng.rocketmq.quickstart;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import com.zhufeng.rocketmq.constant.Constant;

public class Producer {

	public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		DefaultMQProducer producer = new DefaultMQProducer("test_quick_producerGroup");
		producer.setNamesrvAddr(Constant.NAMESRVADDR);
		producer.setSendMsgTimeout(100000);
		producer.start();
		
		Message message = new Message("test_quick_topic_master", "test_quick_tag"
				, "test_quick_key1m", ("Hello QuickStart_RocketMQ_Producer1m1s").getBytes());
		
	
		SendResult sr = producer.send(message);
		System.out.println(sr);
		
		producer.shutdown();
	}

}
