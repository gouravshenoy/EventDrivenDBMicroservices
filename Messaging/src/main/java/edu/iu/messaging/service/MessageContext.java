package edu.iu.messaging.service;

/**
 * Created by Ajinkya on 2/2/17.
 */
import org.apache.thrift.TBase;
public class MessageContext {
    private final TBase event;
    private final String messageId;

    public MessageContext(TBase event, String messageId){
        this.event = event;
        this.messageId = messageId;
    }

    public TBase getEvent() {
        return event;
    }

    public String getMessageId() {
        return messageId;
    }
}
