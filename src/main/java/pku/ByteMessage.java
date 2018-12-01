package pku;

/**
 * 字节消息接口
 *
 */
public interface ByteMessage {


    void setHeaders(KeyValue headers);//设置消息头

    byte[] getBody();//获取字节数据

    void setBody(byte[] body);//设置字节数据

    public KeyValue headers();

    //设置header
    public ByteMessage putHeaders(String key, int value);

    public ByteMessage putHeaders(String key, long value);

    public ByteMessage putHeaders(String key, double value);

    public ByteMessage putHeaders(String key, String value) ;

}
