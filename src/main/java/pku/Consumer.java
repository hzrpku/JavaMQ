package pku;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 消费者
 */

public class Consumer {
    List<String> topics = new ArrayList<>();
    String queue;
    int readpos =0;

    //将消费者订阅的topic进行绑定
    public synchronized void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue != null) {
            System.out.println("只能绑定一次");
            System.exit(0);
        }
        queue = queueName; //queue可以绑定到多个topic中
        topics.addAll(t); //所有的topic加入到topics这个list中

    }


    //每次消费读取一个message
    public synchronized ByteMessage poll() {

        return DemoMessageStore.store.pull(queue, topics);

    }

}
