package com.zhufeng.rocketmq.pull;

import java.util.List;

import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;

import com.zhufeng.rocketmq.constant.Constant;

public class PullScheduleService {
	public static void main(String[] args) throws MQClientException {

		final MQPullConsumerScheduleService service = new MQPullConsumerScheduleService("test_pull_service_group");

		service.getDefaultMQPullConsumer().setNamesrvAddr(Constant.NAMESRVADDR);

		service.setMessageModel(MessageModel.CLUSTERING);

		service.registerPullTaskCallback("test_pull_topic", (mq, context) -> {
			MQPullConsumer consumer = context.getPullConsumer();

			System.err.println("---------------queueId: " + mq.getQueueId() + "----------------");

			// 获取从哪里拉取
			try {
				long offset = consumer.fetchConsumeOffset(mq, false);
				if (offset < 0)
					offset = 0;
				PullResult pullResult = consumer.pull(mq, "*", offset, 32);
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
				case OFFSET_ILLEGAL:
					break;
				default:
					break;
				}

				consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
				// 设置再过3000ms后重新拉取
				context.setPullNextDelayTimeMillis(3000);
			} catch (RemotingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MQBrokerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MQClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		service.start();
	}
}
