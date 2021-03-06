package pku;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;



public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();
	static HashMap<String, DataOutputStream> files = new HashMap<>();
	HashMap<String, DataInputStream> bufferinput = new HashMap<>();



	public void flush() throws IOException {
		for (String topic : files.keySet()) {
			files.get(topic).flush();
		}

	}
	/**************push**************/
	public void push(ByteMessage msg, String topic) throws Exception {


		byte[] body;
		DataOutputStream dataout;
		synchronized (files) {
			if (!files.containsKey(topic)) {
				dataout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("data/" + topic, true), 28 * 1024));
				files.put(topic, dataout);
			}
		}
			dataout = files.get(topic);


		byte bodytype;

		if (msg.getBody().length>512){
			body =msg2byte(msg.getBody());
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
			dataout.writeLong((Long)header.get ("BornTimestamp"));
			dataout.writeLong((Long)header.get("StoreTimestamp"));
			dataout.writeLong((Long)header.get("StartTime"));
			dataout.writeLong((Long)header.get("StopTime"));
			dataout.writeDouble((Double)header.get ("ShardingKey"));
			dataout.writeDouble((Double)header.get("ShardingPartition"));
			dataout.writeUTF(header.getOrDefault("BornHost","null")+","+
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


		if (!bufferinput.containsKey(topic)) {
			File file = new File("data/" + topic);
			if (!file.exists()) {
				return null;
			}

			DataInputStream datain = new DataInputStream(new BufferedInputStream(new FileInputStream("data/" + topic),16*1024));
			bufferinput.put(topic, datain);

		}
		DataInputStream datain = bufferinput.get(topic);
/*******************read*************************/

		int typebody = datain.read();//读类型
		if (typebody == -1) {
			return null;
		}
		DefaultMessage msg = new DefaultMessage();
		KeyValue header = msg.headers();


		header.put("MessageId",datain.readInt());//读头部
		header.put("Timeout",datain.readInt());
		header.put("Priority",datain.readInt());
		header.put("Reliability",datain.readInt());
		header.put("BornTimestamp",datain.readLong());
		header.put("StoreTimestamp",datain.readLong());
		header.put("StartTime",datain.readLong());
		header.put("StopTime",datain.readLong());
		header.put("ShardingKey",datain.readDouble());
		header.put("ShardingPartition",datain.readDouble());

		String[] Headers = datain.readUTF().split(",");
		header.put("BornHost",Headers[0]);
		header.put("StoreHost",Headers[1]);
		header.put("SearchKey",Headers[2]);
		header.put("ScheduleExpression",Headers[3]);
		header.put("TraceId",Headers[4]);
		msg.setHeaders(header);

		if (typebody==1) {
			bodylenth = datain.readShort();//读body
			bodycontent = new byte[bodylenth];
			datain.read(bodycontent);
			msg.setBody(byte2msg(bodycontent));
			return msg;
		}
		else {
			bodylenth = datain.readByte();
			bodycontent = new byte[bodylenth];
			datain.read(bodycontent);
			msg.setBody(bodycontent);
			return msg;
		}

	}

	public static byte[] msg2byte(byte[] data) {
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

	public static byte[] byte2msg(byte[] data) {
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