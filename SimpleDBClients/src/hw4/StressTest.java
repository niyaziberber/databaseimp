package hw4;

import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

public class StressTest {

    public static void main(String[] args) {

        // makes sure startup creates a server called "hw4server"
        SimpleDB.init("hw4server");

        // thread create count
        int cnt = 10;

        Thread[] threads = new Thread[cnt];
        //create new threads
        for(int i=0; i<cnt; i++){
            Thread t = new Thread(new TransactionCreateRunnable());
            threads[i] = t;
            t.start();
        }

        // join them all
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Runnable that creates a transaction and sleeps for 10 sec.
     * Never closes the transaction.
     * during 10 seconds, crash the server manually to check.
     */
    public static class TransactionCreateRunnable implements Runnable {
        public void run() {
            Connection conn = null;
            try {
                Driver d = new SimpleDriver();
                conn = d.connect("jdbc:simpledb://localhost", null);
                Transaction t = new Transaction();
                System.out.println("Thread " + Thread.currentThread().getId() + " sleeping");
                Thread.sleep(10000);
                t.commit();
            }
            catch(SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
