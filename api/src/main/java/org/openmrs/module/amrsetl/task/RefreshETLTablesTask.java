package org.openmrs.module.amrsetl.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.openmrs.api.context.Context;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.scheduler.tasks.AbstractTask;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

public class RefreshETLTablesTask extends AbstractTask {
	
	private Log log = LogFactory.getLog(getClass());
	
	/**
	 * @see AbstractTask#execute()
	 */
	public void execute() {
		Context.openSession();
		
		DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);
		
		Transaction tx = null;
		try {
			
			tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
			final Transaction finalTx = tx;
			sf.getCurrentSession().doWork(new Work() {
				
				@Override
				public void execute(Connection connection) throws SQLException {
					
					CallableStatement cs = connection.prepareCall("{call sp_scheduled_updates}");
					cs.execute();
					
				}
			});
			finalTx.commit();
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Unable to execute query", e);
		}
		finally {
			Context.closeSession();
		}
	}
	
}
