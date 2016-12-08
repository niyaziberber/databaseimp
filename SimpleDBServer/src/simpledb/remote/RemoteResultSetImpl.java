package simpledb.remote;

import simpledb.record.Schema;
import simpledb.query.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteResultSet.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
class RemoteResultSetImpl extends UnicastRemoteObject implements RemoteResultSet {
   private Scan s;
   private Schema sch;
   private RemoteConnectionImpl rconn;
   private boolean wasNullFlag = false;

   /**
    * Creates a RemoteResultSet object.
    * The specified plan is opened, and the scan is saved.
    * @param plan the query plan
    * @param rconn TODO
    * @throws RemoteException
    */
   public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rconn) throws RemoteException {
      s = plan.open();
      sch = plan.schema();
      this.rconn = rconn;
   }

   /**
    * Moves to the next record in the result set,
    * by moving to the next record in the saved scan.
    * @see simpledb.remote.RemoteResultSet#next()
    */
   public boolean next() throws RemoteException {
		try {
	      return s.next();
      }
      catch(RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }

   /**
    * Returns the integer value of the specified field,
    * by returning the corresponding value on the saved scan.
    * @see simpledb.remote.RemoteResultSet#getInt(java.lang.String)
    */
   public int getInt(String fldname) throws RemoteException {
		try {
	      fldname = fldname.toLowerCase(); // to ensure case-insensitivity
           if (checkAndSetNullFlag(fldname))
              return 0;
	      return s.getInt(fldname);
      }
      catch(RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }

   /**
    * Returns the integer value of the specified field,
    * by returning the corresponding value on the saved scan.
    * @see simpledb.remote.RemoteResultSet#getInt(java.lang.String)
    */
   public String getString(String fldname) throws RemoteException {
		try {
	      fldname = fldname.toLowerCase(); // to ensure case-insensitivity
           if (checkAndSetNullFlag(fldname))
              return "";
           return s.getString(fldname);
      }
      catch(RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }

   /**
    * Returns the result set's metadata,
    * by passing its schema into the RemoteMetaData constructor.
    * @see simpledb.remote.RemoteResultSet#getMetaData()
    */
   public RemoteMetaData getMetaData() throws RemoteException {
      return new RemoteMetaDataImpl(sch);
   }

   /**
    * Closes the result set by closing its scan.
    * @see simpledb.remote.RemoteResultSet#close()
    */
   public void close() throws RemoteException {
      s.close();
      rconn.commit();
   }

   /**
    * helper function to check if the value is null in specified field.
    * If it is null, then the wasNull flag will be set to true, otherwise false.
    * @param fldname name of the field.
    * @return true if flag value is null
    */
   private boolean checkAndSetNullFlag(String fldname) throws RemoteException {
      boolean flag = s.isNull(fldname);
      wasNullFlag = flag;
      return flag;
   }

   public boolean wasNull() throws RemoteException {
      return wasNullFlag;
   }
}

