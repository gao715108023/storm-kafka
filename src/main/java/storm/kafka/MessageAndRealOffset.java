package storm.kafka;

import kafka.message.Message;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class MessageAndRealOffset {

    private Message msg;

    private long offset;

    public MessageAndRealOffset(Message msg, long offset) {
        this.msg = msg;
        this.offset = offset;
    }

    public Message getMsg() {
        return msg;
    }

    public long getOffset() {
        return offset;
    }
}
