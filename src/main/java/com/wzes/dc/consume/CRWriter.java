package com.wzes.dc.consume;

import com.datastax.driver.core.*;
import com.wzes.dc.bean.Task;
import com.wzes.dc.produce.Producer;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BufferedRandomAccessFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.wzes.dc.produce.Producer.PRODUCE_FILENAME;

/**
 * @author Create by xuantang
 * @date on 11/6/17
 */
public class CRWriter {

    public static void main(String[] args) {
        long s = System.currentTimeMillis();
        long m = 0L;
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoint("148.100.92.156")
                    .withPort(4392)
                    .withClusterName("tongji01")
                    .withCredentials("user22", "1552730")
                    .build();
            Session session = cluster.connect();
            session.execute("use keyspace_user22");
            int i = 1002;
            BatchStatement batch = new BatchStatement();
            PreparedStatement ps = session
                    .prepare("insert into number(number_id) values(?)");
            while (i < 1000) {
                for (int j = 0; j < 10; j++) {
                    BoundStatement bs = ps.bind(i++);
                    batch.add(bs);
                }
                session.execute(batch);
                batch.clear();
            }
            m = System.currentTimeMillis();
            ResultSet rs = session.execute("select count(*) from number");
            Long number = rs.one().getLong("count");
            System.out.println(number);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        finally {
            if (cluster != null) {
                cluster.close();
            }
        }
        long e = System.currentTimeMillis();
        System.out.println(Thread.currentThread().toString() + " Select Time: " +  (e - m) + " ms");
        System.out.println(Thread.currentThread().toString() + " Total Time: " +  (e - s) + " ms");
    }
}
