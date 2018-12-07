package pku;

import java.util.HashMap;

public class MessageHeaderType {
    public static final HashMap<Byte, String> headerReadIB = new HashMap<Byte, String>() {
        {
            put((byte) 1, "MessageId");
            put((byte) 2, "Topic");
            put((byte) 3, "BornTimestamp");
            put((byte) 4, "BornHost");
            put((byte) 5, "StoreTimestamp");
            put((byte) 6, "StoreHost");
            put((byte) 7, "StartTime");
            put((byte) 8, "StopTime");
            put((byte) 9, "Timeout");
            put((byte) 10, "Priority");
            put((byte) 11, "Reliability");
            put((byte) 12, "SearchKey");
            put((byte) 13, "ScheduleExpression");
            put((byte) 14, "ShardingKey");
            put((byte) 15, "ShardingPartition");
            put((byte) 16, "TraceId");
        }
    };

    public static final HashMap<String, Short> headerType = new HashMap<String, Short>() {
        {
            put("MessageId", (short) 3);
            put("Topic", (short) 4);
            put("BornTimestamp", (short) 1);
            put("BornHost", (short) 4);
            put("StoreTimestamp", (short) 1);
            put("StoreHost", (short) 4);
            put("StartTime", (short) 1);
            put("StopTime", (short) 1);
            put("Timeout", (short) 3);
            put("Priority", (short) 3);
            put("Reliability", (short) 3);
            put("SearchKey", (short) 4);
            put("ScheduleExpression", (short) 4);
            put("ShardingKey", (short) 2);
            put("ShardingPartition", (short) 2);
            put("TraceId", (short) 4);
        }
    };

    public static final HashMap<String, Byte> headerTypeByte = new HashMap<String, Byte>() {
        {
            put("MessageId", (byte) 1);
            put("Topic", (byte) 2);
            put("BornTimestamp", (byte) 3);
            put("BornHost", (byte) 4);
            put("StoreTimestamp", (byte) 5);
            put("StoreHost", (byte) 6);
            put("StartTime", (byte) 7);
            put("StopTime", (byte) 8);
            put("Timeout", (byte) 9);
            put("Priority", (byte) 10);
            put("Reliability", (byte) 11);
            put("SearchKey", (byte) 12);
            put("ScheduleExpression", (byte) 13);
            put("ShardingKey", (byte) 14);
            put("ShardingPartition", (byte) 15);
            put("TraceId", (byte) 16);
        }
    };

    public static final HashMap<Byte, Short> headerReadTB = new HashMap<Byte, Short>() {
        {
            put((byte) 1, (short) 3);
            put((byte) 2, (short) 4);
            put((byte) 3, (short) 1);
            put((byte) 4, (short) 4);
            put((byte) 5, (short) 1);
            put((byte) 6, (short) 4);
            put((byte) 7, (short) 1);
            put((byte) 8, (short) 1);
            put((byte) 9, (short) 3);
            put((byte) 10, (short) 3);
            put((byte) 11, (short) 3);
            put((byte) 12, (short) 4);
            put((byte) 13, (short) 4);
            put((byte) 14, (short) 2);
            put((byte) 15, (short) 2);
            put((byte) 16, (short) 4);
        }
    };
}
