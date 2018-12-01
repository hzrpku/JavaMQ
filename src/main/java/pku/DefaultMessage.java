package pku;

/**
 *消息对象的实现
 */
public class DefaultMessage implements ByteMessage{ //实现一个接口

    private KeyValue headers = new DefaultKeyValue(); //headers为一个
    private byte[] body;

    public void setHeaders(KeyValue headers) {
        this.headers = headers;
    }

    public DefaultMessage(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public KeyValue headers() {
        return headers;
    }


    public DefaultMessage putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

}
