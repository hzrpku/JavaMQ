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

			dataout.writeInt(msg.headers().getInt("MessageId"));//写头部
			dataout.writeInt(msg.headers().getInt("Timeout"));
			dataout.writeInt(msg.headers().getInt("Priority"));
			dataout.writeInt(msg.headers().getInt("Reliability"));
			dataout.writeLong(msg.headers().getLong("BornTimestamp"));
			dataout.writeLong(msg.headers().getLong("StoreTimestamp"));
			dataout.writeLong(msg.headers().getLong("StartTime"));
			dataout.writeLong(msg.headers().getLong("StopTime"));
			dataout.writeDouble(msg.headers().getDouble("ShardingKey"));
			dataout.writeDouble(msg.headers().getDouble("ShardingPartition"));
			dataout.writeShort(msg.headers().getString("Topic").getBytes().length);
			dataout.write(msg.headers().getString("Topic").getBytes());
			dataout.writeShort(msg.headers().getString("BornHost").getBytes().length);
			dataout.write(msg.headers().getString("BornHost").getBytes());
			dataout.writeShort(msg.headers().getString("StoreHost").getBytes().length);
			dataout.write(msg.headers().getString("StoreHost").getBytes());
			dataout.writeShort(msg.headers().getString("SearchKey").getBytes().length);
			dataout.write(msg.headers().getString("SearchKey").getBytes());
			dataout.writeShort(msg.headers().getString("ScheduleExpression").getBytes().length);
			dataout.write(msg.headers().getString("ScheduleExpression").getBytes());
			dataout.writeShort(msg.headers().getString("TraceId").getBytes().length);
			dataout.write(msg.headers().getString("TraceId").getBytes());

			dataout.writeShort(body.length);//写body
			dataout.write(body);
		}


	}

	/**************pull******************/
	ByteMessage pull(String topic) throws IOException {
		byte[] bodycontent;
		byte[] topbyte;
		byte[] borbyte;
		byte[] stobyte;
		byte[] seabyte;
		byte[] schbyte;
		byte[] trabyte;

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
		short top = bufferin.readShort();
		topbyte = new byte[top];
		bufferin.read(topbyte);
		msg.putHeaders(MessageHeader.TOPIC,new String(topbyte));
		short bor = bufferin.readShort();
		borbyte = new byte[bor];
		bufferin.read(borbyte);
		msg.putHeaders(MessageHeader.BORN_HOST,new String(borbyte));
		short sto = bufferin.readShort();
		stobyte = new byte[sto];
		bufferin.read(stobyte);
		msg.putHeaders(MessageHeader.STORE_HOST,new String(stobyte));
		short sea = bufferin.readShort();
		seabyte = new byte[sea];
		bufferin.read(seabyte);
		msg.putHeaders(MessageHeader.SEARCH_KEY,new String(seabyte));
		short sch = bufferin.readShort();
		schbyte = new byte[sch];
		bufferin.read(schbyte);
		msg.putHeaders(MessageHeader.SCHEDULE_EXPRESSION,new String(schbyte));
		short tra = bufferin.readShort();
		trabyte = new byte[tra];
		bufferin.read(trabyte);
		msg.putHeaders(MessageHeader.TRACE_ID,new String(trabyte));

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