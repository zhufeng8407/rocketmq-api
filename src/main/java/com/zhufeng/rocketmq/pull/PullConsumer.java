package com.zhufeng.rocketmq.pull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import com.zhufeng.rocketmq.constant.Constant;

public class PullConsumer {

	private static final Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();

	public static void main(String[] args)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		new PullConsumer().start();
	}

	private void start() throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("test_pull_consumer_group");

		consumer.setNamesrvAddr(Constant.NAMESRVADDR);

		consumer.start();

		// 从指定topic去获取所有的队列
		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("test_pull_topic");

		for (MessageQueue mq : mqs) {
			System.out.println("consume from queue: " + mq);

			SINGLE_MQ: while (true) {
				PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
				System.out.println(pullResult);
				System.out.println(pullResult.getPullStatus());
				System.out.println();
				putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

				switch (pullResult.getPullStatus()) {
				case FOUND:
					List<MessageExt> list = pullResult.getMsgFoundList();
					for (MessageExt msg : list) {
						System.out.println(new String(msg.getBody()));
					}
					break;
				case NO_MATCHED_MSG:
					break;
				case NO_NEW_MSG:
					break;
				case OFFSET_ILLEGAL:
					break;
				default:
					break;
				}
			}
		}

		consumer.shutdown();
	}

	private long getMessageQueueOffset(MessageQueue mq) {
		Long offset = offsetTable.get(mq);

		if (offset != null)
			return offset;

		return 0;
	}

	private void putMessageQueueOffset(MessageQueue mq, long offset) {
		offsetTable.put(mq, offset);

	}

}
