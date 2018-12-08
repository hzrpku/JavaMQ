package pku;

import java.io.*;

import java.util.HashMap;
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
			KeyValue header = msg.headers();
			dataout.writeInt(header.getInt("MessageId"));//写头部
			dataout.writeInt(header.getInt("Timeout"));
			dataout.writeInt(header.getInt("Priority"));
			dataout.writeInt(header.getInt("Reliability"));
			dataout.writeLong(header.getLong("BornTimestamp"));
			dataout.writeLong(header.getLong("StoreTimestamp"));
			dataout.writeLong(header.getLong("StartTime"));
			dataout.writeLong(header.getLong("StopTime"));
			dataout.writeDouble(header.getDouble("ShardingKey"));
			dataout.writeDouble(header.getDouble("ShardingPartition"));

			dataout.writeUTF(header.getString("Topic"));
			dataout.writeUTF(header.getString("BornHost"));
			dataout.writeUTF(header.getString("StoreHost"));
			dataout.writeUTF(header.getString("SearchKey"));
			dataout.writeUTF(header.getString("ScheduleExpression"));
			dataout.writeUTF(header.getString("TraceId"));

			dataout.writeShort(body.length);//写body
			dataout.write(body);
		}


	}

	/**************pull******************/
	ByteMessage pull(String topic) throws IOException {
		byte[] bodycontent;
		byte[] topbyte;
		byte[] borbyte;

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