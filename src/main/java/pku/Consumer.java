package pku;

import java.util.*;

/**
 * 消费者
 */

public class Consumer {
    //List<String> topics = new ArrayList<>();
    HashMap<String,ArrayList<String>> topics = new HashMap<>();
    String queue;
    //int readpos =0;

    //将消费者订阅的topic进行绑定
    public synchronized void attachQueue(String queueName, ArrayList<String> t) throws Exception {
        if (topics.containsKey(queueName)) {
            System.out.println("只能绑定一次");
            System.exit(0);
        }
      //  queue = queueName; //queue可以绑定到多个topic中
        //topics.addAll(t); //所有的topic加入到topics这个list中
        topics.put(queueName,t);
        queue = queueName;

    }


    //每次消费读取一个message
    public synchronized DefaultMessage poll() {
        return DemoMessageStore.store.pull(queue, topics.get(queue));

    }

}
