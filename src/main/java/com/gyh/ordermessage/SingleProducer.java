package com.gyh.ordermessage;

import java.util.List;
  
import com.alibaba.rocketmq.client.exception.MQBrokerException;  
import com.alibaba.rocketmq.client.exception.MQClientException;  
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;  
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;  
import com.alibaba.rocketmq.client.producer.SendResult;  
import com.alibaba.rocketmq.common.message.Message;  
import com.alibaba.rocketmq.common.message.MessageQueue;  
import com.alibaba.rocketmq.remoting.exception.RemotingException;  
  
/** 
 * SingleProducer，发送顺序消息
 */  
public class SingleProducer {
    public static void main(String[] args) {  
        try {  
            DefaultMQProducer producer = new DefaultMQProducer("order_Producer");  
            producer.setNamesrvAddr("192.168.59.128:9876;192.168.59.129:9876");
  
            producer.start();  
  
            // String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD",  
            // "TagE" };  
  
            for (int i = 1; i <= 5; i++) {  
  
                Message msg = new Message("TopicOrderTest", "order_1", "KEY" + i, ("order_1 " + i).getBytes());  
  
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {  
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {  
                        Integer id = (Integer) arg;  
                        int index = id % mqs.size();  
                        return mqs.get(index);  
                    }  
                }, 0);  
  
                System.out.println(sendResult);  
            }  
  
            producer.shutdown();  
        } catch (MQClientException e) {  
            e.printStackTrace();  
        } catch (RemotingException e) {  
            e.printStackTrace();  
        } catch (MQBrokerException e) {  
            e.printStackTrace();  
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        }  
    }  
}