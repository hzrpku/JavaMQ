package pku;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 消费者
 */

public class Consumer {
    List<String> topics = new LinkedList<>();
    int readPos = 0;
    String queue;

    //将消费者订阅的topic进行绑定
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
        queue = queueName; //queue可以绑定到多个topic中
        topics.addAll(t); //所有的topic加入到topics这个list中
        //System.out.println("I have attached");
    }


    //每次消费读取一个message
    public ByteMessage poll() {
        ByteMessage re = null ;

        //先读第一个topic, 再读第二个topic...
        //直到所有topic都读完了, 返回null, 表示无消息
       // for (int i = 0; i < topics.size(); i++) {
        //    int index = (i + readPos) % topics.size();//%10
            re = DemoMessageStore.store.pull(queue, topics);
            //System.out.println(topics+"----");
      //      if (re != null) {
        //        readPos = index + 1;
        //        break;
        //    }
      //  }
        return re;
    }

}
