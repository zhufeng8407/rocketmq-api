package com.zhufeng.rocketmq.pull;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import com.zhufeng.rocketmq.constant.Constant;

public class Producer {

	public static void main(String[] args)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		new Producer().start();
	}

	private void start() throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

		DefaultMQProducer producer = new DefaultMQProducer("test_pull_group");

		producer.setNamesrvAddr(Constant.NAMESRVADDR);
		producer.setSendMsgTimeout(100000);

		producer.start();

		for (int i = 0; i < 10; i++) {
			Message msg = new Message("test_pull_topic", "tagA", ("TEST_PULL_TOPIC_BODY_" + i).getBytes());

			SendResult result = producer.send(msg);

			System.out.println(result);
			
			Thread.sleep(1000);

		}

		producer.shutdown();

	}

}
