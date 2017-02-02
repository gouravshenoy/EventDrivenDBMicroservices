package edu.iu.customer.service.dao.impl;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import edu.iu.customer.service.dao.EntityDAO;
import edu.iu.customer.service.entity.Customer;
import edu.iu.customer.service.entity.Orders;

public class EntityDAOImpl implements EntityDAO {

	Logger logger = LogManager.getLogger(EntityDAOImpl.class);
	
	@Override
	public void saveEntity(Object entity) throws Exception {
		try {
			logger.info("Saving entity in database. Entity: " + entity);
			// Connection details loaded from persistence.xml to create EntityManagerFactory.
			EntityManagerFactory emf = Persistence.createEntityManagerFactory("jpa-customer");

			EntityManager em = emf.createEntityManager();

			// Creating a new transaction.
			EntityTransaction tx = em.getTransaction();

			tx.begin();

			// Persisting the entity object.
			em.merge(entity);

			// Committing transaction.
			tx.commit();
			
			logger.info("DB persist successful; closing connections now!");

			// Closing connection.
			em.close();
			emf.close();
		} catch (Exception ex) {
			logger.error("Error persisting entity in database. Error: " + ex.getMessage(), ex);
			throw ex;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Customer> getCustomers() throws Exception {
		List<Customer> customers = null;
		
		try {
			// Connection details loaded from persistence.xml to create EntityManagerFactory.
			EntityManagerFactory emf = Persistence.createEntityManagerFactory("jpa-customer");

			EntityManager em = emf.createEntityManager();

			// Creating a new transaction.
			EntityTransaction tx = em.getTransaction();

			tx.begin();

			Query query = em.createQuery("SELECT c FROM Customer c");
			customers = query.getResultList();

			// Committing transaction.
			tx.commit();

			// Closing connection.
			em.close();
			emf.close();
		} catch (Exception ex) {
			logger.error("Error getting customers from database. Error: " + ex.getMessage(), ex);
			throw ex;
		}
		return customers;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Orders> getOrdersForCustomer(Integer customerId) throws Exception {
		List<Orders> orders = null;
		try {
			// Connection details loaded from persistence.xml to create EntityManagerFactory.
			EntityManagerFactory emf = Persistence.createEntityManagerFactory("jpa-customer");

			EntityManager em = emf.createEntityManager();

			// Creating a new transaction.
			EntityTransaction tx = em.getTransaction();

			tx.begin();

			Query query = em.createQuery("SELECT o FROM Order o WHERE o.customer.ID=" + customerId);
			orders = query.getResultList();

			// Committing transaction.
			tx.commit();

			// Closing connection.
			em.close();
			emf.close();
		} catch (Exception ex) {
			logger.error("Error getting orders from database. Error: " + ex.getMessage(), ex);
			throw ex;
		}
		return orders;
	}
	
	public static void main(String[] args) {
		EntityDAO dao = new EntityDAOImpl();
		
		// create customer
//		Customer customer = new Customer();
//		customer.setCustomerName("Customer-A");
//		customer.setCreditLimit(5000);
		
		try {
			// persist in db
//			dao.saveEntity(customer);
//			System.out.println("Customer record saved!");
			
			// get list of customers
			List<Customer> customers = dao.getCustomers();
			System.out.println(customers);
		} catch (Exception ex) {
			System.err.println("Exception occured: " + ex);
		}
	}
}
