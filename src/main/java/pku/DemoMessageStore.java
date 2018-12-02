package pku;

import java.io.*;
import java.util.*;

/**
 * 这是一个消息队列的内存实现
 */
public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();
	File file = new File("data/topic");
	FileOutputStream out;
	FileInputStream in;
	static BufferedOutputStream bufferout;   //static
	BufferedInputStream bufferin;
	//给每个consumer对应一个流
	HashMap<String,BufferedInputStream> inMap = new HashMap<>();

	// 消息存储
	//HashMap<String, ArrayList<ByteMessage>> msgs = new HashMap<>(); //msgs存储消息，为一个topic内的所有消息
	// 遍历指针
	HashMap<String, Integer> readPos = new HashMap<>();


	// 加锁保证线程安全
	/**
	 * @param msg
	 * @param topic
	 */
	public synchronized void push(ByteMessage msg, String topic) {
		if (msg == null) {
			return;
		}
		try{

			if(out == null) {
				//StringBuilder builder = new StringBuilder();
				out = new FileOutputStream(file, true);
				bufferout = new BufferedOutputStream(out);
			}
				KeyValue headers = msg.headers();
				Set<String> keyS = headers.keySet();
				Iterator<String> it = keyS.iterator();
				while (it.hasNext()){
					String key = it.next();
					if (!key.equals(MessageHeader.TOPIC)) {

						bufferout.write((byte)key.getBytes().length); //存key长度
						bufferout.write(key.getBytes());//存key
						bufferout.write((byte)msg.headers().getString(key).getBytes().length);//存value长度
						bufferout.write(msg.headers().getString(key).getBytes());//存value

					}
				}
				bufferout.write((byte)(topic.getBytes().length + msg.getBody().length + 2));//存总长度，为一个字节
			    bufferout.write((byte) topic.getBytes().length);//存topic长度信息，为一个字节
			    bufferout.write(topic.getBytes());
			    bufferout.write((byte) msg.getBody().length);//存body长度信息,为一个字节
			    bufferout.write(msg.getBody());
			    //bufferout.flush();


		}catch (IOException e){
			e.printStackTrace();
		}

	}


	// 加锁保证线程安全
	public synchronized ByteMessage pull(String queue, List<String> topics) {
		try {
			if (!inMap.containsKey(queue)) {
				in = new FileInputStream(file);
				bufferin = new BufferedInputStream(in);
				inMap.put(queue, bufferin);
			}
			//每个queue都有一个InputStream
			//********** 第四处 ************
			bufferin = inMap.get(queue);

			//********** 第四处 ************
			if (bufferin.available() ==0) {
				return null;
			}
			byte[] byteTopic;
			byte[] body;
			byte[] key1,key2,key3,key4;
			byte[] value1,value2,value3,value4;
			String Skey1,Skey2,Skey3,Skey4;
			String Svalue1,Svalue2,Svalue3,Svalue4;
			//每次循环读一个message的数据量
			do {

				byte key1len = (byte)bufferin.read();
				if (key1len==-1)
					return null;
				key1 = new byte[key1len];
				bufferin.read(key1);
				Skey1 = new String(key1);
				byte value1len = (byte)bufferin.read();
				value1 = new byte[value1len];
				bufferin.read(value1);
				Svalue1 = new String(value1);

				byte key2len = (byte)bufferin.read();
				key2 = new byte[key2len];
				bufferin.read(key2);
				Skey2 = new String(key2);
				byte value2len = (byte)bufferin.read();
				value2 = new byte[value2len];
				bufferin.read(value2);
				Svalue2 = new String(value2);

				byte key3len = (byte)bufferin.read();
				key3 = new byte[key3len];
				bufferin.read(key3);
				Skey3 = new String(key3);
				byte value3len = (byte)bufferin.read();
				value3 = new byte[value3len];
				bufferin.read(value3);
				Svalue3 = new String(value3);

				/*byte key4len = (byte)bufferin.read();
				key4 = new byte[key4len];
				bufferin.read(key4);
				Skey4 = new String(key4);
				byte value4len = (byte)bufferin.read();
				value4 = new byte[value4len];
				bufferin.read(value4);
				Svalue4 = new String(value4);*/

				byte lenTotal = (byte) bufferin.read();
				//读到文件尾了，则lenTotal为-1
				//if(lenTotal==-1)
					//return null;

				byte[] byteTotal = new byte[lenTotal];
				bufferin.read(byteTotal);
				byte lenTopic = byteTotal[0];
				byteTopic = new byte[lenTopic];
				System.arraycopy(byteTotal, 1, byteTopic, 0, lenTopic);//chucuo
				body = new byte[lenTotal - 2 - lenTopic];
				System.arraycopy(byteTotal,lenTopic+2,body,0,lenTotal - 2 - lenTopic);



				//********** 第五处 **********
			} while (!topics.contains(new String(byteTopic)));

			ByteMessage msg = new DefaultMessage(body);
			msg.putHeaders(Skey1,Svalue1);
			msg.putHeaders(Skey2,Svalue2);
			msg.putHeaders(Skey3,Svalue3);
			//msg.putHeaders(Skey4,Svalue4);
			return msg;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	
}
