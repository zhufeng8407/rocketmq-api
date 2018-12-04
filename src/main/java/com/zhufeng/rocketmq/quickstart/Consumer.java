package com.zhufeng.rocketmq.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import com.zhufeng.rocketmq.constant.Constant;

public class Consumer {

	public static void main(String[] args) throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_quick_consumerGroup");

		consumer.setNamesrvAddr(Constant.NAMESRVADDR);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

		// 消费者订阅
		consumer.subscribe("test_quick_topic_master", "*");

		// register listener
		consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
			MessageExt msg = msgs.get(0);
			try {

				System.err.println(
						"topic is: " + msg.getTopic() + ", tag is: " + msg.getTags() + ", key is: " + msg.getKeys()
								+ ", message body is: " + new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));

				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			} catch (Exception e) {
				int recentTimes = msg.getReconsumeTimes();
				if (recentTimes < 3) {
					System.out.println();
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

			}
		});

		consumer.start();

	}

}
