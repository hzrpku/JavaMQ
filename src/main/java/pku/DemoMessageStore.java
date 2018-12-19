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
				dataout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("data/" + topic, true),20*1024));
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

		String toc = topic + Thread.currentThread().getName();
		if (!bufferinput.containsKey(toc)) {
			File file = new File("data/" + topic);
			if (!file.exists()) {
				return null;
			}

			DataInputStream datain = new DataInputStream(new BufferedInputStream(new FileInputStream("data/" + topic),20*1024));
			bufferinput.put(toc, datain);

		}
		DataInputStream datain = bufferinput.get(toc);
/*******************read*************************/

		int typebody = datain.read();//读类型
		if (typebody == -1) {
			return null;
		}
		DefaultMessage msg = new DefaultMessage();


		msg.putHeaders("MessageId",datain.readInt());//读头部
		msg.putHeaders("Timeout",datain.readInt());
		msg.putHeaders("Priority",datain.readInt());
		msg.putHeaders("Reliability",datain.readInt());
		msg.putHeaders("BornTimestamp",datain.readLong());
		msg.putHeaders("StoreTimestamp",datain.readLong());
		msg.putHeaders("StartTime",datain.readLong());
		msg.putHeaders("StopTime",datain.readLong());
		msg.putHeaders("ShardingKey",datain.readDouble());
		msg.putHeaders("ShardingPartition",datain.readDouble());

		String[] Headers = datain.readUTF().split(",");
		msg.putHeaders("BornHost",Headers[0]);
		msg.putHeaders("StoreHost",Headers[1]);
		msg.putHeaders("SearchKey",Headers[2]);
		msg.putHeaders("ScheduleExpression",Headers[3]);
		msg.putHeaders("TraceId",Headers[4]);

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
}