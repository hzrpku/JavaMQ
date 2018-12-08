package pku;

import java.io.*;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();
	HashMap<String, DataOutputStream> files = new HashMap<>();
	HashMap<String, DataInputStream> bufferInput = new HashMap<>();


	public void flush() throws IOException {
		for (String file : files.keySet()) {
			files.get(file).close();
		}

	}
	/**************push**************/
	public void push(ByteMessage msg, String topic) throws Exception {

		byte[] byteheader;
		byte[] lenofheader;
		byte[] body;
		byte[] lenofbody;

		DataOutputStream dataout;
		synchronized (files) {
			if (!files.containsKey(topic)) {
				dataout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("data/" + topic, true)));
				files.put(topic, dataout);
			}

			dataout = files.get(topic);

		}

		byteheader = header(msg.headers());//得到header字节
		lenofheader = intTobyte(byteheader.length);
		byte bodytype;
		if (msg.getBody().length>2048){
			body = msg2byte_gzip(msg.getBody());
			bodytype=1;
		}
		else{
			body=msg.getBody();
			bodytype=0;
		}
		synchronized (dataout) {
			dataout.write(lenofheader);
			dataout.write(byteheader);
			dataout.writeByte(bodytype);
			dataout.writeShort(body.length);
			dataout.write(body);
		}


	}

	/**************pull******************/
	ByteMessage pull(String topic) throws IOException {
		byte[] byteheader;
		byte[] headercontent;
		byte[] byteBodyLength;
		byte[] bodycontent;
		String header;

		String toc = topic + Thread.currentThread().getName();
		if (!bufferInput.containsKey(toc)) {
			File file = new File("data/" + topic);
			if (!file.exists()) {
				return null;
			}

			FileInputStream in = new FileInputStream("data/" + topic);
			BufferedInputStream bufferIn = new BufferedInputStream(in);
			DataInputStream bufferin = new DataInputStream(bufferIn);
			bufferInput.put(toc, bufferin);

		}
		DataInputStream bufferin = bufferInput.get(toc);
/*******************read*************************/

		byteheader = new byte[4];//读头部
		int ret = bufferin.read(byteheader);
		if (ret == -1) {
			bufferin.close();
			return null;
		}
		int lenofheader = Byte2Int(byteheader);
		headercontent = new byte[lenofheader];
		bufferin.read(headercontent);
		header = new String(headercontent);

		byte typebody = bufferin.readByte(); //读类型

		short bodylen = bufferin.readShort();//读body
		bodycontent = new byte[bodylen];
		bufferin.read(bodycontent);

		if (typebody==1) {
			DefaultMessage msg = new DefaultMessage(byte2msg_gzip(bodycontent));
			DefaultKeyValue keyValue = makeKeyValue(header);
			msg.setHeaders(keyValue);//设置头部
			return msg;
		}else{
			DefaultMessage msg = new DefaultMessage(bodycontent);
			DefaultKeyValue keyValue = makeKeyValue(header);
			msg.setHeaders(keyValue);//设置头部
			return msg;
		}

	}

	private static DefaultKeyValue makeKeyValue(String header) {

		String[] split = header.split(",");

		DefaultKeyValue defaultKeyValue = new DefaultKeyValue();
		defaultKeyValue.put(MessageHeader.MESSAGE_ID, split[0]);
		defaultKeyValue.put(MessageHeader.TOPIC, split[1]);
		defaultKeyValue.put(MessageHeader.BORN_TIMESTAMP, split[2]);
		defaultKeyValue.put(MessageHeader.BORN_HOST, split[3]);
		defaultKeyValue.put(MessageHeader.STORE_TIMESTAMP, split[4]);
		defaultKeyValue.put(MessageHeader.STORE_HOST, split[5]);
		defaultKeyValue.put(MessageHeader.START_TIME, split[6]);
		defaultKeyValue.put(MessageHeader.STOP_TIME, split[7]);
		defaultKeyValue.put(MessageHeader.TIMEOUT, split[8]);
		defaultKeyValue.put(MessageHeader.PRIORITY, split[9]);
		defaultKeyValue.put(MessageHeader.RELIABILITY, split[10]);
		defaultKeyValue.put(MessageHeader.SEARCH_KEY, split[11]);
		defaultKeyValue.put(MessageHeader.SCHEDULE_EXPRESSION, split[12]);
		defaultKeyValue.put(MessageHeader.SHARDING_KEY, split[13]);
		defaultKeyValue.put(MessageHeader.SHARDING_PARTITION, split[14]);
		defaultKeyValue.put(MessageHeader.TRACE_ID, split[15]);

		return defaultKeyValue;

	}


	public synchronized static int Byte2Int(byte[] bytes) {
		return (bytes[0] & 0xff) << 24
				| (bytes[1] & 0xff) << 16
				| (bytes[2] & 0xff) << 8
				| (bytes[3] & 0xff);
	}

	private synchronized static byte[] intTobyte(int num) {
		byte[] bytes = new byte[4];
		bytes[0] = (byte) ((num >> 24) & 0xff);
		bytes[1] = (byte) ((num >> 16) & 0xff);
		bytes[2] = (byte) ((num >> 8) & 0xff);
		bytes[3] = (byte) (num & 0xff);
		return bytes;
	}


	private static byte[] header(KeyValue headers) {

		Map<String, Object> map = headers.getMap();
		String result = String.valueOf(map.get(MessageHeader.MESSAGE_ID)) + "," +
				map.get(MessageHeader.TOPIC) + "," +
				map.get(MessageHeader.BORN_TIMESTAMP) + "," +
				map.get(MessageHeader.BORN_HOST) + "," +
				map.get(MessageHeader.STORE_TIMESTAMP) + "," +
				map.get(MessageHeader.STORE_HOST) + "," +
				map.get(MessageHeader.START_TIME) + "," +
				map.get(MessageHeader.STOP_TIME) + "," +
				map.get(MessageHeader.TIMEOUT) + "," +
				map.get(MessageHeader.PRIORITY) + "," +
				map.get(MessageHeader.RELIABILITY) + "," +
				map.get(MessageHeader.SEARCH_KEY) + "," +
				map.get(MessageHeader.SCHEDULE_EXPRESSION) + "," +
				map.get(MessageHeader.SHARDING_KEY) + "," +
				map.get(MessageHeader.SHARDING_PARTITION) + "," +
				map.get(MessageHeader.TRACE_ID);
		return result.getBytes();
	}

	//压缩
	public static byte[] msg2byte_gzip(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(bos);
			gzip.write(data);
			//gzip.finish();
			gzip.close();
			b = bos.toByteArray();
			bos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}
	//解压缩
	public static byte[] byte2msg_gzip(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			GZIPInputStream gzip = new GZIPInputStream(bis);
			byte[] buf = new byte[1024];
			int num = -1;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			while ((num = gzip.read(buf, 0, buf.length)) != -1) {
				baos.write(buf, 0, num);
			}
			b = baos.toByteArray();
			//baos.flush();
			baos.close();
			gzip.close();
			bis.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}

}

