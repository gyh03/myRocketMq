package com.gyh.ordermessage;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 顺序消息消费，带事务方式（应用可控制Offset什么时候提交） 
 */  
public class ClusterConsumer1 {
  
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_Consumer");
        consumer.setNamesrvAddr("192.168.59.128:9876;192.168.59.129:9876");
  
        /** 
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br> 
         * 如果非第一次启动，那么按照上次消费的位置继续消费 
         */  
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
  
        consumer.subscribe("TopicOrderTest", "*");  
          
        /** 
         * 实现了MessageListenerOrderly表示一个队列只会被一个线程取到  
         *，第二个线程无法访问这个队列 
         */  
        consumer.registerMessageListener(new MessageListenerOrderly() {
            AtomicLong consumeTimes = new AtomicLong(0);
  
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                // 设置自动提交  
                context.setAutoCommit(true);  
                for (MessageExt msg : msgs) {  
                    System.out.println(Thread.currentThread().getName()+" > " + msg + ",内容：" + new String(msg.getBody()));
                }  
  
                try {  
                    TimeUnit.SECONDS.sleep(3L);
                } catch (InterruptedException e) {  
  
                    e.printStackTrace();  
                }
                if(msgs.get(0).getKeys().equals("KEY3_2")){
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;  
            }  
        });  
  
        consumer.start();  
  
        System.out.println("ClusterConsumer1 Started.");
    }  
  
}