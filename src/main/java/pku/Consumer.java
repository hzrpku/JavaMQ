package pku;

import java.io.IOException;
import java.util.*;
import java.util.zip.DataFormatException;

public class Consumer {
    DemoMessageStore store = new DemoMessageStore();
    List<String> topics = new LinkedList<>();
    String queue;
    int readpos =0;



    //将消费者订阅的topic进行绑定
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue!=null) {
            System.out.println("只能绑定一次");
            System.exit(0);
        }
        //  queue = queueName; //queue可以绑定到多个topic中
        //topics.addAll(t); //所有的topic加入到topics这个list中
        topics.addAll(t);
        queue = queueName;
        //System.out.println(queue+topics);

    }


    //每次消费读取一个message
    public ByteMessage poll()throws IOException, DataFormatException {
        ByteMessage re ;
        re = store.pull(topics.get(readpos));
        if (re == null) {
            readpos++;
            if (readpos < topics.size()) {
                return poll();
            }
            return null;

        }
        else {
            return re;
        }


    }
}
