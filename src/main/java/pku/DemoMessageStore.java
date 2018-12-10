package pku;

import java.io.*;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();
	HashMap<String, DataOutputStream> files = new HashMap<>();
	HashMap<String, DataInputStream> bufferinput = new HashMap<>();


	public void flush() throws IOException {
		for (String file : files.keySet()) {
			files.get(file).close();
		}

	}
	/**************push**************/
	public void push(ByteMessage msg, String topic) throws Exception {


		byte[] body;

		DataOutputStream dataout;
		synchronized (files) {
			if (!files.containsKey(topic)) {
				dataout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("data/" + topic, true)));
				files.put(topic, dataout);
			}

			dataout = files.get(topic);

		}
		byte bodytype;

		if (msg.getBody().length>3072){
			body = msg2byte_gzip(msg.getBody());
			bodytype=1;
		}
		else{
			body=msg.getBody();
			bodytype=0;
		}
		KeyValue head = msg.headers();
		Map<String,Object> header = head.getMap();
		synchronized (dataout) {
			dataout.writeByte(bodytype);//写类型
			dataout.writeInt((Integer)header.get("MessageId"));//写头部
			dataout.writeInt((Integer)header.get("Timeout"));
			dataout.writeInt((Integer)header.get("Priority"));
			dataout.writeInt((Integer)header.get("Reliability"));
			dataout.writeLong((Long)header.get("BornTimestamp"));
			dataout.writeLong((Long)header.get("StoreTimestamp"));
			dataout.writeLong((Long)header.get("StartTime"));
			dataout.writeLong((Long)header.get("StopTime"));
			dataout.writeDouble((Double)header.get("ShardingKey"));
			dataout.writeDouble((Double)header.get("ShardingPartition"));

			dataout.writeUTF((String)header.get("Topic"));
			dataout.writeUTF((String)header.getOrDefault("BornHost","null"));
			dataout.writeUTF((String)header.getOrDefault("StoreHost","null"));
			dataout.writeUTF((String)header.getOrDefault("SearchKey","null"));
			dataout.writeUTF((String)header.getOrDefault("ScheduleExpression","null"));
			dataout.writeUTF((String)header.getOrDefault("TraceId","null"));

			dataout.writeShort(body.length);//写body
			dataout.write(body);
		}


	}

	/**************pull******************/
	ByteMessage pull(String topic) throws IOException {
		byte[] bodycontent;

		String toc = topic + Thread.currentThread().getName();
		if (!bufferinput.containsKey(toc)) {
			File file = new File("data/" + topic);
			if (!file.exists()) {
				return null;
			}

			FileInputStream in = new FileInputStream("data/" + topic);
			BufferedInputStream bufferIn = new BufferedInputStream(in);
			DataInputStream bufferin = new DataInputStream(bufferIn);
			bufferinput.put(toc, bufferin);

		}
		DataInputStream bufferin = bufferinput.get(toc);
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

	public static byte[] msg2byte_gzip(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(bos);
			gzip.write(data);
			gzip.close();
			b = bos.toByteArray();
			bos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}

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
			baos.close();
			gzip.close();
			bis.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}

}