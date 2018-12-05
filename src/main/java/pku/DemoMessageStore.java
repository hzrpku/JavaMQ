package pku;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这是一个消息队列的内存实现
 */
public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();
	File file = new File("data/topic");
	FileOutputStream out;
	static BufferedOutputStream bufferout;

	private synchronized static byte[]intToByte(int num){
		byte[]bytes=new byte[4];
		bytes[0]=(byte) ((num>>24)&0xff);
		bytes[1]=(byte) ((num>>16)&0xff);
		bytes[2]=(byte) ((num>>8)&0xff);
		bytes[3]=(byte) (num&0xff);
		return bytes;
	}



//*********************//
	// 加锁保证线程安全
	/**
	 * @param msg
	 * @param topic
	 */


	public synchronized void push(ByteMessage msg, String topic) {
		byte[] valuelen,valuelen1;

		if (msg == null) {
			return;
		}
		try{

			if(out == null) {
				out = new FileOutputStream(file, true);
				bufferout = new BufferedOutputStream(out);
			}
				KeyValue headers = msg.headers();
				Set<String> keyS = headers.keySet();
				Iterator<String> it = keyS.iterator();


			bufferout.write((byte)topic.getBytes().length);//存topic长度信息，为一个字节
			bufferout.write(topic.getBytes());
			int lenth1 = msg.getBody().length;
			//System.out.println(lenth1);
			valuelen1 = intToByte(lenth1);
			bufferout.write((byte)valuelen1.length);
			bufferout.write(valuelen1);
			bufferout.write(msg.getBody());

				while (it.hasNext()){
					String key = it.next();
					if (!key.equals(MessageHeader.TOPIC)) {

						bufferout.write((byte)key.getBytes().length); //存key长度
						bufferout.write(key.getBytes());//存key
						int length = msg.headers().getString(key).getBytes().length;//得到value的字节长度
						valuelen = intToByte(length); //value长度转化为字节数组
						bufferout.write((byte)valuelen.length);//存此字节数组的长度
						bufferout.write(valuelen);//存此字节数组
						bufferout.write(msg.headers().getString(key).getBytes());//存value

					}
				}

			bufferout.flush();


		}catch (IOException e){
			e.printStackTrace();
		}

	}



	
}
