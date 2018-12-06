package pku;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;



public class DemoMessageStore {


	static HashMap<String, ArrayList<ByteMessage>> msgs = new HashMap<>();

	static HashMap<String, BufferedInputStream> bufferInput = new HashMap<>();

	static AtomicInteger count = new AtomicInteger(0);


	static void push(ByteMessage msg, String topic) throws Exception {


		if (count.get() > 500000) {
			save();
			msgs.clear();
			count.set(0);
		}


		if (!msgs.containsKey(topic)) {
			msgs.put(topic, new ArrayList<>());
		}

		msgs.get(topic).add(msg);
		count.incrementAndGet();
	}

	 ByteMessage pull(String topic) throws IOException{

		String toc = topic + Thread.currentThread().getName();///
		//System.out.println(toc);
		if (!bufferInput.containsKey(toc)) {
			File file = new File("data/"+topic);
			if (!file.exists()){
				return null;
			}

			FileInputStream fis = new FileInputStream("data/"+topic);
			BufferedInputStream bis = new BufferedInputStream(fis);
			bufferInput.put(toc, bis);

		}
		BufferedInputStream bufferedInputStream = bufferInput.get(toc);

		byte[] byteHeaderLength;
		byte[] headerContent;
		byte[] byteBodyLength;
		byte[] bodyContent;
		String header;

		byteHeaderLength = new byte[4];
		int ret = bufferedInputStream.read(byteHeaderLength);
		//System.out.println(ret);
		int intHeaderLength = Byte2Int(byteHeaderLength);

		if (intHeaderLength == 0 || ret == -1) {
			bufferedInputStream.close();
			return null;
		}

		headerContent = new byte[intHeaderLength];
		bufferedInputStream.read(headerContent);
		header = new String(headerContent);

		byteBodyLength = new byte[4];
		bufferedInputStream.read(byteBodyLength);
		int intBodyLength = Byte2Int(byteBodyLength);

		if (intBodyLength == 0) {
			return null;
		}

		bodyContent = new byte[intBodyLength];
		bufferedInputStream.read(bodyContent);

		DefaultKeyValue keyValue = makeKeyValue(header);
		DefaultMessage message = new DefaultMessage(bodyContent);

		message.setHeaders(keyValue);
		return message;

	}

	private static DefaultKeyValue makeKeyValue(String header) {

		String[] split = header.split(",");

		if (split.length != 16) {
			return null;
		}

		DefaultKeyValue defaultKeyValue = new DefaultKeyValue();

		if (!split[0].equals("0"))
			defaultKeyValue.put(MessageHeader.MESSAGE_ID, split[0]);

		if (!split[1].equals("0"))
			defaultKeyValue.put(MessageHeader.TOPIC, split[1]);

		if (!split[2].equals("0"))
			defaultKeyValue.put(MessageHeader.BORN_TIMESTAMP, split[2]);

		if (!split[3].equals("0"))
			defaultKeyValue.put(MessageHeader.BORN_HOST, split[3]);

		if (!split[4].equals("0"))
			defaultKeyValue.put(MessageHeader.STORE_TIMESTAMP, split[4]);

		if (!split[5].equals("0"))
			defaultKeyValue.put(MessageHeader.STORE_HOST, split[5]);

		if (!split[6].equals("0"))
			defaultKeyValue.put(MessageHeader.START_TIME, split[6]);

		if (!split[7].equals("0"))
			defaultKeyValue.put(MessageHeader.STOP_TIME, split[7]);

		if (!split[8].equals("0"))
			defaultKeyValue.put(MessageHeader.TIMEOUT, split[8]);

		if (!split[9].equals("0"))
			defaultKeyValue.put(MessageHeader.PRIORITY, split[9]);

		if (!split[10].equals("0"))
			defaultKeyValue.put(MessageHeader.RELIABILITY, split[10]);

		if (!split[11].equals("0"))
			defaultKeyValue.put(MessageHeader.SEARCH_KEY, split[11]);

		if (!split[12].equals("0"))
			defaultKeyValue.put(MessageHeader.SCHEDULE_EXPRESSION, split[12]);

		if (!split[13].equals("0"))
			defaultKeyValue.put(MessageHeader.SHARDING_KEY, split[13]);

		if (!split[14].equals("0"))
			defaultKeyValue.put(MessageHeader.SHARDING_PARTITION, split[14]);

		if (!split[15].equals("0"))
			defaultKeyValue.put(MessageHeader.TRACE_ID, split[15]);

		return defaultKeyValue;

	}

	private static void save() throws Exception {

		FileOutputStream fos;
		BufferedOutputStream bos;

		for (String topic : msgs.keySet()) {

			fos = new FileOutputStream("data/" + topic, true);
			bos = new BufferedOutputStream(fos);


			ArrayList<ByteMessage> byteMessages = msgs.get(topic);
			for (ByteMessage message : byteMessages) {

				byte[] header = header(message.headers());
				byte[] headerLength = intToByte(header.length);
				byte[] body = message.getBody();
				byte[] bodyLength = intToByte(body.length);

				bos.write(headerLength);
				bos.write(header);
				bos.write(bodyLength);
				bos.write(body);

			}
			bos.flush();
			fos.close();
			bos.close();
		}


	}


	public static int Byte2Int(byte[]bytes) {
		return (bytes[0]&0xff)<<24
				| (bytes[1]&0xff)<<16
				| (bytes[2]&0xff)<<8
				| (bytes[3]&0xff);
	}


	private static byte[] header(KeyValue headers) {

		Map<String, Object> map = headers.getMap();
		String result = String.valueOf(map.getOrDefault(MessageHeader.MESSAGE_ID, "0")) + "," +
				map.getOrDefault(MessageHeader.TOPIC, "0") + "," +
				map.getOrDefault(MessageHeader.BORN_TIMESTAMP, "0") + "," +
				map.getOrDefault(MessageHeader.BORN_HOST, "0") + "," +
				map.getOrDefault(MessageHeader.STORE_TIMESTAMP, "0") + "," +
				map.getOrDefault(MessageHeader.STORE_HOST, "0") + "," +
				map.getOrDefault(MessageHeader.START_TIME, "0") + "," +
				map.getOrDefault(MessageHeader.STOP_TIME, "0") + "," +
				map.getOrDefault(MessageHeader.TIMEOUT, "0") + "," +
				map.getOrDefault(MessageHeader.PRIORITY, "0") + "," +
				map.getOrDefault(MessageHeader.RELIABILITY, "0") + "," +
				map.getOrDefault(MessageHeader.SEARCH_KEY, "0") + "," +
				map.getOrDefault(MessageHeader.SCHEDULE_EXPRESSION, "0") + "," +
				map.getOrDefault(MessageHeader.SHARDING_KEY, "0") + "," +
				map.getOrDefault(MessageHeader.SHARDING_PARTITION, "0") + "," +
				map.getOrDefault(MessageHeader.TRACE_ID, "0");
		return result.getBytes();
	}


	private synchronized static byte[]intToByte(int num){
		byte[]bytes=new byte[4];
		bytes[0]=(byte) ((num>>24)&0xff);
		bytes[1]=(byte) ((num>>16)&0xff);
		bytes[2]=(byte) ((num>>8)&0xff);
		bytes[3]=(byte) (num&0xff);
		return bytes;
	}


	public static void lastsave() throws Exception {
		save();
		msgs.clear();
		count.set(0);
	}
}

