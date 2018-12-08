package pku;

/**
 * 生产者
 */
public class Producer {
    static int count=0;
    static DemoMessageStore store = new DemoMessageStore();



	//生成一个指定topic的message返回
    public  ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        ByteMessage msg=new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    //将message发送出去
    public  void send(ByteMessage defaultMessage)throws Exception{
        String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);

        store.push(defaultMessage, topic);

    }
    //处理将缓存区的剩余部分
    public void flush()throws Exception {
        count++;

        if (count==4)
        store.flush();

    }
}
