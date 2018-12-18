package pku;

import java.io.*;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
//import java.util.zip.ZipOutputStream;
//import java.util.zip.ZipInputStream;
//import java.util.zip.ZipEntry;


public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();
	HashMap<String, DataOutputStream> files = new HashMap<>();
	HashMap<String, DataInputStream> bufferinput = new HashMap<>();


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

		if (msg.getBody().length>1024){
			body =msg2byte_gzip(msg.getBody());
			bodytype=1;
		}
		else{
			body=msg.getBody();
			bodytype=0;
		}
		KeyValue head = msg.headers();
		Map<String,Object> header = head.getMap();
		synchronized (dataout) { //同步同一对象
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
			dataout.writeUTF(header.get("Topic")+","+
					header.getOrDefault("BornHost","null")+","+
					header.getOrDefault("StoreHost","null")+","+
					header.getOrDefault("SearchKey","null")+","+
					header.getOrDefault("ScheduleExpression","null")+","+
					header.getOrDefault("TraceId","null"));
			if (bodytype==1) {
				dataout.writeShort(body.length);//写body
			}
			else {
				dataout.writeByte(body.length);
			}
			dataout.write(body);
		}


	}

	/**************pull******************/
	ByteMessage pull(String topic) throws IOException {
		byte[] bodycontent;
		short bodylenth;
		DataInputStream datain;

		String toc = topic + Thread.currentThread().getName();
		if (!bufferinput.containsKey(toc)) {
			File file = new File("data/" + topic);
			if (!file.exists()) {
				return null;
			}

			FileInputStream in = new FileInputStream("data/" + topic);
			BufferedInputStream bufferIn = new BufferedInputStream(in);
			datain = new DataInputStream(bufferIn);
			bufferinput.put(toc, datain);

		}
		datain = bufferinput.get(toc);
/*******************read*************************/

		int typebody = datain.read();//读类型
		if (typebody == -1) {
			return null;
		}
		DefaultMessage msg = new DefaultMessage();

		msg.putHeaders(MessageHeader.MESSAGE_ID,datain.readInt());//读头部
		msg.putHeaders(MessageHeader.TIMEOUT,datain.readInt());
		msg.putHeaders(MessageHeader.PRIORITY,datain.readInt());
		msg.putHeaders(MessageHeader.RELIABILITY,datain.readInt());
		msg.putHeaders(MessageHeader.BORN_TIMESTAMP,datain.readLong());
		msg.putHeaders(MessageHeader.STORE_TIMESTAMP,datain.readLong());
		msg.putHeaders(MessageHeader.START_TIME,datain.readLong());
		msg.putHeaders(MessageHeader.STOP_TIME,datain.readLong());
		msg.putHeaders(MessageHeader.SHARDING_KEY,datain.readDouble());
		msg.putHeaders(MessageHeader.SHARDING_PARTITION,datain.readDouble());

		String[] Headers = datain.readUTF().split(",");
		msg.putHeaders(MessageHeader.TOPIC,Headers[0]);
		msg.putHeaders(MessageHeader.BORN_HOST,Headers[1]);
		msg.putHeaders(MessageHeader.STORE_HOST,Headers[2]);
		msg.putHeaders(MessageHeader.SEARCH_KEY,Headers[3]);
		msg.putHeaders(MessageHeader.SCHEDULE_EXPRESSION,Headers[4]);
		msg.putHeaders(MessageHeader.TRACE_ID,Headers[5]);
		if (typebody==1) {
			bodylenth = datain.readShort();//读body
		}
		else {
			bodylenth = datain.readByte();
		}
		bodycontent = new byte[bodylenth];
		datain.read(bodycontent);

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
/*
	public static byte[] zip(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ZipOutputStream zip = new ZipOutputStream(bos);
			ZipEntry entry = new ZipEntry("zip");
			entry.setSize(data.length);
			zip.putNextEntry(entry);
			zip.write(data);
			zip.closeEntry();
			zip.close();
			b = bos.toByteArray();
			bos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}

	public static byte[] unZip(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ZipInputStream zip = new ZipInputStream(bis);
			while (zip.getNextEntry() != null) {
				byte[] buf = new byte[1024];
				int num = -1;
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				while ((num = zip.read(buf, 0, buf.length)) != -1) {
					baos.write(buf, 0, num);
				}
				b = baos.toByteArray();
				baos.flush();
				baos.close();
			}
			zip.close();
			bis.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}
	*/

}