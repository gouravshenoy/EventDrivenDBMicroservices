package edu.iu.customer.service.handler;

import java.util.ArrayList;
import java.util.List;

import edu.iu.messaging.service.MessageContext;
import edu.iu.messaging.service.core.MessagingFactory;
import edu.iu.messaging.service.core.Publisher;
import edu.iu.messaging.service.core.Subscriber;
import edu.iu.messaging.service.util.Constants;
import edu.iu.messaging.service.util.Type;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.iu.customer.service.adapter.JPAThriftAdapter;
import edu.iu.customer.service.dao.EntityDAO;
import edu.iu.customer.service.dao.impl.EntityDAOImpl;
import edu.iu.customer.service.model.Customer;
import edu.iu.customer.service.model.CustomerService;
import edu.iu.customer.service.model.OperationFailedException;


public class CustomerServiceHandler implements CustomerService.Iface {
	
	private static final EntityDAO DAO = new EntityDAOImpl();
	private static final Logger logger = LogManager.getLogger(CustomerServiceHandler.class);
	private Publisher customerPublisher;
	private Subscriber orderSubscriber;

	public CustomerServiceHandler(){
		customerPublisher = MessagingFactory.getPublisher(Type.CUSTOMER);
		orderSubscriber = MessagingFactory.getSubscriber(new OrderMessageHandler(), getRoutingKeys(), Type.ORDER);
	}

	public List<String> getRoutingKeys(){
		return new ArrayList<String>(){{add(Constants.ORDER_ROUTING_KEY);}};
	}

	@Override
	public List<Customer> getCustomers() throws OperationFailedException, TException {
		List<Customer> customers = new ArrayList<>();
		try {
			// get orders list from db
			List<edu.iu.customer.service.entity.Customer> customerList = DAO.getCustomers();
			logger.info("Fetched customers from DB, JPA list: " + customerList);
			
			// convert from JPA to Thrift
			for (edu.iu.customer.service.entity.Customer customer : customerList) {
				customers.add(JPAThriftAdapter.getCustomerThriftDM(customer));
			}
		} catch (Exception ex) {
			logger.error("getOrders | exception: " + ex.getMessage(), ex);
			throw new OperationFailedException("Get Orders operation failed: " + ex.getMessage());
		}
		logger.info("Returning list (thrift converted");
		return customers;
	}

	@Override
	public void createCustomer(Customer customer) throws OperationFailedException, TException {
		try {
			// save customer in db
			if (customer != null) {

				logger.info("Creating customer entry in DB: " + customer);
				DAO.saveEntity(JPAThriftAdapter.getCustomerJPAEntity(customer));

				logger.info("Publishing new customer to outside world: " + customer);
				MessageContext mctx = new MessageContext(customer, customer.getCustomerName());
				customerPublisher.publish(mctx);

			} else {
				throw new Exception ("Customer object null");
			}
		} catch (Exception ex) {
			logger.error("createCustomer | exception: " + ex.getMessage(), ex);
			throw new OperationFailedException("Create customer operation failed: " + ex.getMessage());
		}
	}

}
