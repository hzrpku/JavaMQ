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
			files.get(file).flush();
		}

	}
	/**************push**************/
	public void push(ByteMessage msg, String topic) throws Exception {

		byte[] byteheader;
		byte[] lenofheader;
		byte[] body;
		//byte[] lenofbody;

		DataOutputStream dataout;
		synchronized (files) {
			if (!files.containsKey(topic)) {
				dataout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("data/" + topic, true)));
				files.put(topic, dataout);
			}

			dataout = files.get(topic);

		}


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
			dataout.writeByte(bodytype);//写类型

			dataout.writeInt(msg.headers().getInt(MessageHeader.MESSAGE_ID));//写头部
			dataout.writeInt(msg.headers().getInt(MessageHeader.TIMEOUT));
			dataout.writeInt(msg.headers().getInt(MessageHeader.PRIORITY));
			dataout.writeInt(msg.headers().getInt(MessageHeader.RELIABILITY));
			dataout.writeLong(msg.headers().getLong(MessageHeader.BORN_TIMESTAMP));
			dataout.writeLong(msg.headers().getLong(MessageHeader.STORE_TIMESTAMP));
			dataout.writeLong(msg.headers().getLong(MessageHeader.START_TIME));
			dataout.writeLong(msg.headers().getLong(MessageHeader.STOP_TIME));
			dataout.writeDouble(msg.headers().getDouble(MessageHeader.SHARDING_KEY));
			dataout.writeDouble(msg.headers().getDouble(MessageHeader.SHARDING_PARTITION));
			dataout.writeUTF(msg.headers().getString(MessageHeader.TOPIC));
			dataout.writeUTF(msg.headers().getString(MessageHeader.BORN_HOST));
			dataout.writeUTF(msg.headers().getString(MessageHeader.STORE_HOST));
			dataout.writeUTF(msg.headers().getString(MessageHeader.SEARCH_KEY));
			dataout.writeUTF(msg.headers().getString(MessageHeader.SCHEDULE_EXPRESSION));
			dataout.writeUTF(msg.headers().getString(MessageHeader.TRACE_ID));

			dataout.writeShort(body.length);//写body
			dataout.write(body);
		}


	}

	/**************pull******************/
	ByteMessage pull(String topic) throws IOException {
		byte[] bodycontent;

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

		int typebody = bufferin.read();//读类型
		if (typebody == -1) {
			bufferin.close();
			return null;
		}
		DefaultMessage msg = new DefaultMessage();

		msg.putHeaders(MessageHeader.MESSAGE_ID,bufferin.readInt());//读头部
		msg.putHeaders(MessageHeader.TIMEOUT,bufferin.readInt());
		msg.putHeaders(MessageHeader.PRIORITY,bufferin.readInt());
		msg.putHeaders(MessageHeader.RELIABILITY,bufferin.readInt());
		msg.putHeaders(MessageHeader.BORN_TIMESTAMP,bufferin.readLong());
		msg.putHeaders(MessageHeader.STORE_TIMESTAMP,bufferin.readLong());
		msg.putHeaders(MessageHeader.START_TIME,bufferin.readLong());
		msg.putHeaders(MessageHeader.STOP_TIME,bufferin.readLong());
		msg.putHeaders(MessageHeader.SHARDING_KEY,bufferin.readDouble());
		msg.putHeaders(MessageHeader.SHARDING_PARTITION,bufferin.readDouble());
		msg.putHeaders(MessageHeader.TOPIC,bufferin.readUTF());
		msg.putHeaders(MessageHeader.BORN_HOST,bufferin.readUTF());
		msg.putHeaders(MessageHeader.STORE_HOST,bufferin.readUTF());
		msg.putHeaders(MessageHeader.SEARCH_KEY,bufferin.readUTF());
		msg.putHeaders(MessageHeader.SCHEDULE_EXPRESSION,bufferin.readUTF());
		msg.putHeaders(MessageHeader.TRACE_ID,bufferin.readUTF());

		short bodylenth = bufferin.readShort();//读body
		bodycontent = new byte[bodylenth];
		bufferin.read(bodycontent);

		if (typebody==1) {
			msg.setBody(byte2msg_gzip(bodycontent));
			return msg;
		}else{
			msg.setBody(bodycontent);
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

