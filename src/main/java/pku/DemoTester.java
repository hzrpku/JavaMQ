package pku;

import pku.ByteMessage;
import pku.Consumer;
import pku.MessageHeader;
import pku.Producer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这个程序演示了测评程序的基本逻辑
 * 正式的测评程序会比这个更复杂
 */
public class DemoTester {
    //每个pusher向每个topic发送的消息数目
    static int PUSH_COUNT =100000;
    //发送消息的线程数
    static int PUSH_THREAD_COUNT = 4;
    //发送线程往n个topic发消息
    static int PUSH_TOPIC_COUNT = 10;
    //消费消息的线程数
    static int PULL_THREAD_COUNT = 4;
    //每个消费者消费的topic数量
    static int PULL_TOPIC_COUNT = 10;
    //topic数量
    static int TOPIC_COUNT = 20;
    //每个queue绑定的topic数量
    //统计push/pull消息的数量
    static AtomicInteger pushCount = new AtomicInteger();
    static AtomicInteger pullCount = new AtomicInteger();

    static class PushTester implements Runnable {
        //随机向以下topic发送消息
        List<String> topics = new ArrayList<>();
        Producer producer = new Producer();
        int id;

        PushTester(List<String> t, int id) {
            topics.addAll(t);
            this.id = id;
            StringBuilder sb=new StringBuilder();
            sb.append(String.format("producer%d push to:",id));
            for (int i = 0; i <t.size() ; i++) {
                sb.append(t.get(i)+" ");
            }
            System.out.println(sb.toString());
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < topics.size(); i++) { //对于每一个topic
                    String topic = topics.get(i);
                    for (int j = 0; j < PUSH_COUNT; j++) {//每个pusher向每个topic发送的消息数目,为100
                        //topic加j作为数据部分
                        //j是序号, 在consumer中会用来校验顺序
                        byte[] data = (topic +" "+id + " " + j).getBytes();
                        ByteMessage msg = producer.createBytesMessageToTopic(topics.get(i), data);
                        //设置一个header
                        msg.putHeaders(MessageHeader.SEARCH_KEY, "1");
                        msg.putHeaders(MessageHeader.SHARDING_KEY, 11.0);
                        msg.putHeaders(MessageHeader.BORN_TIMESTAMP, 12233);
                        msg.putHeaders(MessageHeader.PRIORITY, "2");
                        msg.putHeaders(MessageHeader.BORN_HOST,"a3?");
                        msg.putHeaders(MessageHeader.TRACE_ID,"sssd");
                        msg.putHeaders(MessageHeader.MESSAGE_ID,123);
                        msg.putHeaders(MessageHeader.RELIABILITY,"ssa");
                        msg.putHeaders(MessageHeader.SCHEDULE_EXPRESSION,"wed");
                        msg.putHeaders(MessageHeader.SHARDING_PARTITION,"ds");
                        msg.putHeaders(MessageHeader.START_TIME,"ssd");
                        msg.putHeaders(MessageHeader.STOP_TIME,"saaa");
                        msg.putHeaders(MessageHeader.TIMEOUT,"sdsdd");
                        msg.putHeaders(MessageHeader.STORE_TIMESTAMP,23);
                        msg.putHeaders(MessageHeader.STORE_HOST,1123);
                        //发送消息
                        producer.send(msg);
                        pushCount.incrementAndGet();
                    }
                }
                producer.flush();
                //System.out.println("what?");//
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    static class PullTester implements Runnable {
        //拉消息
        String queue;
        List<String> topics = new ArrayList<>();
        Consumer consumer = new Consumer();
        int pc=0;
        public PullTester(String s, ArrayList<String> tops) throws Exception {
            queue = s;
            topics.addAll(tops);
            consumer.attachQueue(s, tops);

            StringBuilder sb=new StringBuilder();
            sb.append(String.format("queue%s attach:",s));
            for (int i = 0; i <topics.size() ; i++) {
                sb.append(topics.get(i)+" ");
            }
            System.out.println(sb.toString());
        }

        @Override
        public void run() {
            try {
                //检查顺序, 保存每个topic-producer对应的序号, 新获得的序号必须严格+1
                HashMap<String, Integer> posTable = new HashMap<>();
                while (true) {//循环
                    ByteMessage msg = consumer.poll();
                    if (msg == null) {
                        System.out.println(String.format("thread pull %s",pc));
                        return;
                    } else {
                        byte[] data = msg.getBody();
                        String str = new String(data);
                        String[] strs = str.split(" ");
                        String topic = strs[0];//topic
                        String prod = strs[1];//线程id
                        int j = Integer.parseInt(strs[2]);//j<=100
                        //System.out.println("topic是"+topic+"  线程:"+prod+"  id为  "+Integer.toString(j));
                        String mapkey=topic+" "+prod;
                        if (!posTable.containsKey(mapkey)) {
                            posTable.put(mapkey, 0);
                        }
                       // System.out.println(j);
                       // System.out.println(posTable.get(mapkey));
                       // System.out.println("---");
                       // System.out.println(pc+"ssss");
                        if (j != posTable.get(mapkey)) {
                            System.out.println(String.format("数据错误 topic %s 序号:%d", topic, j));
                            System.out.println(posTable.get(mapkey)+"   "+j);
                            System.exit(0);
                        }

                        if (!msg.headers().getString(MessageHeader.SEARCH_KEY).equals("1")) {
                            System.out.println(String.format("header错误 topic %s 序号:%d", topic, j));
                            System.exit(0);
                        }
                        if (!(msg.headers().getDouble(MessageHeader.SHARDING_KEY)==11.0)) {
                            System.out.println(String.format("header错误 topic %s 序号:%d", topic, j));
                            System.exit(0);
                        }
                        if (!(msg.headers().getInt(MessageHeader.STORE_TIMESTAMP)==23)) {
                            System.out.println(String.format("header错误 topic %s 序号:%d", topic, j));
                            System.exit(0);
                        }
                        posTable.put(mapkey, posTable.get(mapkey) + 1);
                        pullCount.incrementAndGet();
                        pc++;
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }
    static Random rand = new Random(100);

    static void testPush()throws Exception{
        //topic的名字是topic+序号的形式
        System.out.println("开始push");
        long time1 = System.currentTimeMillis();
        ArrayList<Thread> pushers = new ArrayList<>();
        for (int i = 0; i < PUSH_THREAD_COUNT; i++) {//发送消息的线程数
            //随机选择连续的topic,数量为10
            ArrayList<String> tops = new ArrayList<>();
            int start = rand.nextInt(TOPIC_COUNT);//topic数量
            for (int j = 0; j < PUSH_TOPIC_COUNT; j++) {//对每一个线程,往n个topic发消息,数量为10
                int v = (start+j)%TOPIC_COUNT;//topic数量
                tops.add("topic"+Integer.toString(v));//组成10个topic,topic的名字是topic+序号的形式
            }
            Thread t = new Thread(new PushTester(tops, i));
            t.start();
            pushers.add(t);
        }
        for (int i = 0; i < pushers.size(); i++) {
            pushers.get(i).join();
        }
        long time2 = System.currentTimeMillis();
        System.out.println(String.format("push 结束 time cost %d push count %d", time2 - time1, pushCount.get()));
    }

    static void testPull()throws Exception{
        long time2=System.currentTimeMillis();
        System.out.println("开始pull");
        int queue = 0;
        ArrayList<Thread> pullers = new ArrayList<>();
        for (int i = 0; i < PULL_THREAD_COUNT; i++) {//消费消息的线程数,为4
            //随机选择topic
            ArrayList<String> tops = new ArrayList<>();
            int start = rand.nextInt(TOPIC_COUNT);//topic数量
            for (int j = 0; j < PULL_TOPIC_COUNT; j++) {//每个消费者消费的topic数量,为10
                int v =(start+j)%TOPIC_COUNT;//topic数量
                tops.add("topic"+Integer.toString(v));
            }
            Thread t = new Thread(new PullTester(Integer.toString(queue), tops));
            queue++;//每个消费者建立一个queue，绑定到10个topic中
            t.start();
            pullers.add(t);
        }
        for (int i = 0; i < pullers.size(); i++) {
            pullers.get(i).join();
        }
        long time3 = System.currentTimeMillis();
        System.out.println(String.format("pull 结束 time cost %d pull count %d", time3 - time2, pullCount.get()));
    }


//
    public static void main(String args[]) {
        try {
            File file = new File("data");
            if (file.exists()){
                for (File file1 : file.listFiles()){
                    file1.delete();
                }

            }
                testPush();
                testPull();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
