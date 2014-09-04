package storm.kafka;

import java.io.Serializable;

/**
 * Kafka的Server地址和端口号
 *
 * @author gaochuanjun
 * @since 14-9-3
 */
public class HostPort implements Serializable {

    //IP地址或者主机名
    private String host;
    //端口号
    private int port;

    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * 若只传入IP地址，则使用Kafka的默认端口号9092
     *
     * @param host
     */
    public HostPort(String host) {
        this(host, 9092);
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    @Override
    public boolean equals(Object o) {
        HostPort other = (HostPort) o;
        return host.equals(other.host) && port == other.port;
    }

    @Override
    public int hashCode() {
        return host.hashCode();
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
