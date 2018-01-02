package com.gyh.ordermessage;

import java.util.List;
import java.util.concurrent.TimeUnit;  
import java.util.concurrent.atomic.AtomicLong;   
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;  
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;  
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;  
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;  
import com.alibaba.rocketmq.client.exception.MQClientException;  
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;  
import com.alibaba.rocketmq.common.message.MessageExt;  
  
/** 
 * 顺序消息消费，带事务方式（应用可控制Offset什么时候提交） 
 */  
public class SingleConsumer {
  
    public static void main(String[] args) throws MQClientException {  
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_Consumer");  
        consumer.setNamesrvAddr("192.168.59.128:9876;192.168.59.129:9876");
  
        /** 
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br> 
         * 如果非第一次启动，那么按照上次消费的位置继续消费 
         */  
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);  
  
        consumer.subscribe("TopicOrderTest", "*");  
  
        consumer.registerMessageListener(new MessageListenerOrderly() {  
            AtomicLong consumeTimes = new AtomicLong(0);  
  
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {  
                // 设置自动提交  
                context.setAutoCommit(true);  
                for (MessageExt msg : msgs) {  
                    System.out.println(Thread.currentThread().getName()+" > " + msg + ",内容：" + new String(msg.getBody()));
                }  
  
                try {  
                    TimeUnit.SECONDS.sleep(5L);  
                } catch (InterruptedException e) {
                    e.printStackTrace();  
                }
                //模拟消息异常，KEY1执行异常，他之后的消息也不会执行，将不断重试KEY1消息，发送给其他的consumer或线程
               /* if(msgs.get(0).getKeys().equals("KEY1")){
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }*/
                return ConsumeOrderlyStatus.SUCCESS;  
            }  
        });  
  
        consumer.start();  
  
        System.out.println("SingleConsumer Started.");
    }  
  
}