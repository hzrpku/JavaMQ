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
	FileInputStream in;
	static BufferedOutputStream bufferout;   //static
	BufferedInputStream bufferin;
	//给每个consumer对应一个流
	ConcurrentHashMap<String,BufferedInputStream> inMap = new ConcurrentHashMap<>();

	// 消息存储
	//HashMap<String, ArrayList<ByteMessage>> msgs = new HashMap<>(); //msgs存储消息，为一个topic内的所有消息
	// 遍历指针
	HashMap<String, Integer> readPos = new HashMap<>();
//****************//
	private synchronized static byte[]intToByte(int num){
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
	public synchronized int Byte2Int(byte[]bytes) {
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
				//System.out.println(lenth1);
				valuelen1 = intToByte(lenth1);
				bufferout.write((byte)valuelen1.length);
				bufferout.write(valuelen1);
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
				bufferin = new BufferedInputStream(in,307200);
				inMap.put(queue, bufferin);
			}
			//每个queue都有一个InputStream
			bufferin = inMap.get(queue);

			if (bufferin.available() ==0) {
				return null;
			}
			byte[] byteTopic;
			byte[] body;
			byte[] key1,key2,key3,key4,key5,key6,key7,key8,key9,key10,key11,key12,key13,key14,key15;
			byte[] value1,value2,value3,value4,value5,value6,value7,value8,value9,value10,value11,value12,value13,value14,value15;
			byte[] len1,len2,len3,len4,len5,len6,len7,len8,len9,len10,len11,len12,len13,len14,len15,len;
			String Skey1,Skey2,Skey3,Skey4,Skey5,Skey6,Skey7,Skey8,Skey9,Skey10,Skey11,Skey12,Skey13,Skey14,Skey15;
			String Svalue1,Svalue2,Svalue3,Svalue4,Svalue5,Svalue6,Svalue7,Svalue8,Svalue9,Svalue10,Svalue11,Svalue12,Svalue13,Svalue14,Svalue15;
			//每次循环读一个message的数据量


				byte key1len = (byte)bufferin.read();
				if (key1len==-1)
					return null;
				key1 = new byte[key1len];
				bufferin.read(key1);
				Skey1 = new String(key1);
				byte byteoflen1 = (byte)bufferin.read();
				len1 = new byte[byteoflen1];
				bufferin.read(len1);
				int lenlen = Byte2Int(len1);
				value1 = new byte[lenlen];
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

				byte key5len = (byte)bufferin.read();
				key5 = new byte[key5len];
				bufferin.read(key5);
				Skey5 = new String(key5);
				byte byteoflen5 = (byte)bufferin.read();
				len5 = new byte[byteoflen5];
				bufferin.read(len5);
				int len55 = Byte2Int(len5);
				value5 = new byte[len55];
				bufferin.read(value5);
				Svalue5 = new String(value5);
				//System.out.println(Svalue3);

				byte key6len = (byte)bufferin.read();
				key6 = new byte[key6len];
				bufferin.read(key6);
				Skey6 = new String(key6);
				byte byteoflen6 = (byte)bufferin.read();
				len6 = new byte[byteoflen6];
				bufferin.read(len6);
				int len66 = Byte2Int(len6);
				value6 = new byte[len66];
				bufferin.read(value6);
				Svalue6 = new String(value6);
				//System.out.println(Svalue3);

				byte key7len = (byte)bufferin.read();
				key7 = new byte[key7len];
				bufferin.read(key7);
				Skey7 = new String(key7);
				byte byteoflen7 = (byte)bufferin.read();
				len7 = new byte[byteoflen7];
				bufferin.read(len7);
				int len77 = Byte2Int(len7);
				value7 = new byte[len77];
				bufferin.read(value7);
				Svalue7 = new String(value7);
				//System.out.println(Svalue3);

				byte key8len = (byte)bufferin.read();
				key8 = new byte[key8len];
				bufferin.read(key8);
				Skey8 = new String(key8);
				byte byteoflen8 = (byte)bufferin.read();
				len8 = new byte[byteoflen8];
				bufferin.read(len8);
				int len88 = Byte2Int(len8);
				value8 = new byte[len88];
				bufferin.read(value8);
				Svalue8 = new String(value8);
				//System.out.println(Svalue3);

				byte key9len = (byte)bufferin.read();
				key9 = new byte[key9len];
				bufferin.read(key9);
				Skey9 = new String(key9);
				byte byteoflen9 = (byte)bufferin.read();
				len9 = new byte[byteoflen9];
				bufferin.read(len9);
				int len99 = Byte2Int(len9);
				value9 = new byte[len99];
				bufferin.read(value9);
				Svalue9 = new String(value9);
				//System.out.println(Svalue3);

				byte key10len = (byte)bufferin.read();
				key10 = new byte[key10len];
				bufferin.read(key10);
				Skey10 = new String(key10);
				byte byteoflen10 = (byte)bufferin.read();
				len10 = new byte[byteoflen10];
				bufferin.read(len10);
				int len1010 = Byte2Int(len10);
				value10 = new byte[len1010];
				bufferin.read(value10);
				Svalue10 = new String(value10);
				//System.out.println(Svalue3);

				byte key11len = (byte)bufferin.read();
				key11 = new byte[key11len];
				bufferin.read(key11);
				Skey11 = new String(key11);
				byte byteoflen11 = (byte)bufferin.read();
				len11 = new byte[byteoflen11];
				bufferin.read(len11);
				int len1111 = Byte2Int(len11);
				value11 = new byte[len1111];
				bufferin.read(value11);
				Svalue11 = new String(value11);
				//System.out.println(Svalue3);

				byte key12len = (byte)bufferin.read();
				key12 = new byte[key12len];
				bufferin.read(key12);
				Skey12 = new String(key12);
				byte byteoflen12 = (byte)bufferin.read();
				len12 = new byte[byteoflen12];
				bufferin.read(len12);
				int len1212 = Byte2Int(len12);
				value12 = new byte[len1212];
				bufferin.read(value12);
				Svalue12 = new String(value12);
				//System.out.println(Svalue3);

				byte key13len = (byte)bufferin.read();
				key13 = new byte[key13len];
				bufferin.read(key13);
				Skey13 = new String(key13);
				byte byteoflen13 = (byte)bufferin.read();
				len13 = new byte[byteoflen13];
				bufferin.read(len13);
				int len1313 = Byte2Int(len13);
				value13 = new byte[len1313];
				bufferin.read(value13);
				Svalue13 = new String(value13);
				//System.out.println(Svalue3);

				byte key14len = (byte)bufferin.read();
				key14 = new byte[key14len];
				bufferin.read(key14);
				Skey14= new String(key14);
				byte byteoflen14 = (byte)bufferin.read();
				len14= new byte[byteoflen14];
				bufferin.read(len14);
				int len1414 = Byte2Int(len14);
				value14 = new byte[len1414];
				bufferin.read(value14);
				Svalue14 = new String(value14);
				//System.out.println(Svalue3);

				byte key15len = (byte)bufferin.read();
				key15 = new byte[key15len];
				bufferin.read(key15);
				Skey15 = new String(key15);
				byte byteoflen15 = (byte)bufferin.read();
				len15 = new byte[byteoflen15];
				bufferin.read(len15);
				int len1515 = Byte2Int(len15);
				value15 = new byte[len1515];
				bufferin.read(value15);
				Svalue15 = new String(value15);
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

			ByteMessage msg = new DefaultMessage(body);
			//msg.setBody(body);
			msg.putHeaders(Skey1,Svalue1);
			msg.putHeaders(Skey2,Svalue2);
			msg.putHeaders(Skey3,Svalue3);
			msg.putHeaders(Skey4,Svalue4);
			msg.putHeaders(Skey5,Svalue5);
			msg.putHeaders(Skey6,Svalue6);
			msg.putHeaders(Skey7,Svalue7);
			msg.putHeaders(Skey8,Svalue8);
			msg.putHeaders(Skey9,Svalue9);
			msg.putHeaders(Skey10,Svalue10);
			msg.putHeaders(Skey11,Svalue11);
			msg.putHeaders(Skey12,Svalue12);
			msg.putHeaders(Skey13,Svalue13);
			msg.putHeaders(Skey14,Svalue14);
			msg.putHeaders(Skey15,Svalue15);
			msg.putHeaders(MessageHeader.TOPIC,new String(byteTopic));
			//msg.setBody(body);
			//System.out.println(new String(body));
			return msg;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	
}
