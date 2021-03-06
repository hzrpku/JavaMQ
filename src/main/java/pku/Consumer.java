package pku;

import java.io.IOException;
import java.util.*;


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
        topics.addAll(t);
        queue = queueName;


    }


    //每次消费读取一个message
    public ByteMessage poll()throws IOException {
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
            re.putHeaders("Topic",topics.get(readpos));//put topic
            return re;
        }


    }
}
