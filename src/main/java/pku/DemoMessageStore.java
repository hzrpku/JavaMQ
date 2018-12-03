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
//****************//
	private static byte[]intToByte(int num){
		byte[]bytes=new byte[4];
		bytes[0]=(byte) ((num>>24)&0xff);
		bytes[1]=(byte) ((num>>16)&0xff);
		bytes[2]=(byte) ((num>>8)&0xff);
		bytes[3]=(byte) (num&0xff);
		return bytes;
	}
	/**
	 * byte数组转int类型的对象
	 * @param bytes
	 * @return
	 */
	public int Byte2Int(byte[]bytes) {
		return (bytes[0]&0xff)<<24
				| (bytes[1]&0xff)<<16
				| (bytes[2]&0xff)<<8
				| (bytes[3]&0xff);
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
						int length = msg.headers().getString(key).getBytes().length;//得到value的字节长度
						valuelen = intToByte(length); //value长度转化为字节数组
						bufferout.write((byte)valuelen.length);//存此字节数组的长度
						bufferout.write(valuelen);//存此字节数组
						bufferout.write(msg.headers().getString(key).getBytes());//存value

					}
				}
				bufferout.write((byte)topic.getBytes().length);//存topic长度信息，为一个字节
				bufferout.write(topic.getBytes());
				int lenth1 = msg.getBody().length;
				valuelen1 = intToByte(lenth1);
				bufferout.write((byte)valuelen1.length);
				bufferout.write(valuelen1);
				bufferout.write(msg.getBody());
			//bufferout.flush();


				//bufferout.write((byte)(topic.getBytes().length + msg.getBody().length + 2));//存总长度，为一个字节
			    //bufferout.write((byte) topic.getBytes().length);//存topic长度信息，为一个字节
			    //bufferout.write(topic.getBytes());
			    //bufferout.write((byte) msg.getBody().length);//存body长度信息,为一个字节
			    //bufferout.write(msg.getBody());




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
			bufferin = inMap.get(queue);

		//	if (bufferin.available() ==0) {
		//		return null;
		//	}
			byte[] byteTopic;
			byte[] body;
			byte[] key1,key2,key3,key4;
			byte[] value1,value2,value3,value4;
			byte[] len1,len2,len3,len4,len;
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
				byte byteoflen1 = (byte)bufferin.read();
				len1 = new byte[byteoflen1];
				bufferin.read(len1);
				int len11 = Byte2Int(len1);
				value1 = new byte[len11];
				bufferin.read(value1);
				Svalue1 = new String(value1);
				//System.out.println(Svalue1);

				byte key2len = (byte)bufferin.read();
				key2 = new byte[key2len];
				bufferin.read(key2);
				Skey2 = new String(key2);
				byte byteoflen2 = (byte)bufferin.read();
				len2 = new byte[byteoflen2];
				bufferin.read(len2);
				int len22 = Byte2Int(len2);
				value2 = new byte[len22];
				bufferin.read(value2);
				Svalue2 = new String(value2);
				//System.out.println(Svalue2);

				byte key3len = (byte)bufferin.read();
				key3 = new byte[key3len];
				bufferin.read(key3);
				Skey3 = new String(key3);
				byte byteoflen3 = (byte)bufferin.read();
				len3 = new byte[byteoflen3];
				bufferin.read(len3);
				int len33 = Byte2Int(len3);
				value3 = new byte[len33];
				bufferin.read(value3);
				Svalue3 = new String(value3);
				//System.out.println(Svalue3);

				byte key4len = (byte)bufferin.read();
				key4 = new byte[key4len];
				bufferin.read(key4);
				Skey4 = new String(key4);
				byte byteoflen4 = (byte)bufferin.read();
				len4 = new byte[byteoflen4];
				bufferin.read(len4);
				int len44 = Byte2Int(len4);
				value4 = new byte[len44];
				bufferin.read(value4);
				Svalue4 = new String(value4);
				//System.out.println(Svalue3);

				byte topiclen = (byte)bufferin.read();//topic读取
				byteTopic = new byte[topiclen];
				bufferin.read(byteTopic);

				byte bodylen = (byte)bufferin.read();//body读取
				len = new byte[bodylen];
				bufferin.read(len);
				int lenbody = Byte2Int(len);
				body = new byte[lenbody];
				bufferin.read(body);



				//int lenTotal = (int) bufferin.read();
				//读到文件尾了，则lenTotal为-1
				//if(lenTotal==-1)
					//return null;

				//byte[] byteTotal = new byte[lenTotal];
				//bufferin.read(byteTotal);
				//int lenTopic = byteTotal[0];
				//byteTopic = new byte[lenTopic];
				//System.arraycopy(byteTotal, 1, byteTopic, 0, lenTopic);//chucuo
				//body = new byte[lenTotal - 2 - lenTopic];
				//System.arraycopy(byteTotal,lenTopic+2,body,0,lenTotal - 2 - lenTopic);



				//********** 第五处 **********
			} while (!topics.contains(new String(byteTopic)));

			ByteMessage msg = new DefaultMessage(body);
			msg.headers().put(Skey1,Svalue1);
			msg.headers().put(Skey2,Svalue2);
			msg.headers().put(Skey3,Svalue3);
			msg.headers().put(Skey4,Svalue4);
			msg.headers().put(MessageHeader.TOPIC,new String(byteTopic));
		/*	System.out.println(Skey1);
			System.out.println(Svalue1+"-----");
			System.out.println(Skey2);
			System.out.println(Svalue2+"-----");
			System.out.println(Skey3);
			System.out.println(Svalue3+"-----");
			System.out.println(Skey4);
			System.out.println(Svalue4+"-----");
			System.out.println(new String(body));
			*/
			return msg;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	
}
