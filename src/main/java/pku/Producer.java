package pku;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Set;

/**
 * 生产者
 */
public class Producer {
  //  File file = new File("data");
   // FileOutputStream out;
   // BufferedOutputStream bufferout;


	//生成一个指定topic的message返回
    public synchronized ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        ByteMessage msg=new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    //将message发送出去
    public synchronized void send(ByteMessage defaultMessage){
        String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
        DemoMessageStore.store.push(defaultMessage,topic);//?

    }
    //处理将缓存区的剩余部分
    public void flush()throws Exception{
      //  out = new FileOutputStream(file,true);
       // bufferout = new BufferedOutputStream(out);///
        DemoMessageStore.bufferout.flush();
        //System.out.println("hzr");
    }
}
