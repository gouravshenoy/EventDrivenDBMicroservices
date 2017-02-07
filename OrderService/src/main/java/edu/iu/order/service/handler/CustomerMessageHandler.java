package edu.iu.order.service.handler;

import edu.iu.messaging.service.MessageContext;
import edu.iu.messaging.service.core.MessageHandler;
import edu.iu.messaging.service.model.Customer;
import edu.iu.messaging.service.util.ThriftUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

/**
 * Created by Ajinkya on 2/6/17.
 */
public class CustomerMessageHandler implements MessageHandler {

    /**
     * This method only handle MessageType.PROCESS type messages.
     * @param message
     */
    @Override
    public void onMessage(MessageContext message) {

        try {
            TBase event = message.getEvent();
            byte[] bytes = ThriftUtils.serializeThriftObject(event);
            Customer customer = new Customer();
            ThriftUtils.createThriftFromBytes(bytes, customer);
            //TODO: Replicate customer in Orderservice db
            System.out.println(customer);

        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
