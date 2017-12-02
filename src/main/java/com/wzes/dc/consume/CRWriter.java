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
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.wzes.dc.produce.Producer.PRODUCE_FILENAME;
import static com.wzes.dc.produce.Producer.getFileLength;

/**
 * @author Create by xuantang
 * @date on 11/6/17
 */
public class CRWriter {
    private static final String PRODUCE_FILENAME = "cr_produce.dat";
    private static int threadNumber = 1;

    private final int READ_SIZE = 1024 * 8;

    private static Cluster cluster = null;
    private static Session session = null;

    private final CountDownLatch countDownLatch = new CountDownLatch(threadNumber);

    public static void main(String[] args) throws IOException, InterruptedException {
        cluster = Cluster.builder()
                .addContactPoint("148.100.92.156")
                .withPort(4392)
                .withClusterName("tongji01")
                .withCredentials("user22", "1552730")
                .build();
        session = cluster.connect("keyspace_user22");
        // Multi
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Thread num : " + threadNumber);
            long start = System.currentTimeMillis();

            // produce
            Producer producer = new Producer();
//            if (i < 16) {
//                producer.writeToFileByCompress(PRODUCE_FILENAME);
//            }else {
//                producer.writeByBufferedRandom(PRODUCE_FILENAME);
//            }
            producer.writeByBufferedRandom(PRODUCE_FILENAME);
            long proEnd = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (proEnd - start) + " ms");
            CRWriter crWriter = new CRWriter();
            crWriter.writeToCR();
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - proEnd) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            threadNumber *= 2;
        }

//        long s = System.currentTimeMillis();
//        long m = 0L;
//        Cluster cluster = null;
//        try {
//            cluster = Cluster.builder()
//                    .addContactPoint("148.100.92.156")
//                    .withPort(4392)
//                    .withClusterName("tongji01")
//                    .withCredentials("user22", "1552730")
//                    .build();
//            Session session = cluster.connect();
//            session.execute("use keyspace_user22");
//            int i = 1002;
//            BatchStatement batch = new BatchStatement();
//            PreparedStatement ps = session
//                    .prepare("insert into number(number_id) values(?)");
//            while (i < 1000) {
//                for (int j = 0; j < 10; j++) {
//                    BoundStatement bs = ps.bind(i);
//                    batch.add(bs);
//                }
//                session.execute(batch);
//                batch.clear();
//            }
//            m = System.currentTimeMillis();
//            ResultSet rs = session.execute("select count(*) from number");
//            Long number = rs.one().getLong("count");
//            System.out.println(number);
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//            m = System.currentTimeMillis();
//        }
//        finally {
//            if (cluster != null) {
//                cluster.close();
//            }
//        }
//        long e = System.currentTimeMillis();
//        System.out.println(Thread.currentThread().toString() + " Select Time: " +  (e - m) + " ms");
//        System.out.println(Thread.currentThread().toString() + " Total Time: " +  (e - s) + " ms");
    }


    public void writeToCR() throws IOException, InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);

        Long totalSize = getFileLength(PRODUCE_FILENAME);
        Long sliceSize = totalSize / threadNumber;

        // calculate time
        for(int index = 0; index < threadNumber; index++) {
            BufferedRandomAccessFile readFile = null;

            try {
                readFile = new BufferedRandomAccessFile(PRODUCE_FILENAME, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            WriteToCRThread writeToCRThread = new WriteToCRThread(session, readFile,
                    index * sliceSize, sliceSize);

            executorService.execute(writeToCRThread);
        }
        executorService.shutdown();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * read and write thread
     */
    class WriteToCRThread extends Thread {
        Session session;
        BufferedRandomAccessFile readFile;
        Long startPosition;
        Long length;

        WriteToCRThread(Session session, BufferedRandomAccessFile readFile,
                             Long startPosition, Long length) {
            this.session = session;
            this.readFile = readFile;
            this.startPosition = startPosition;
            this.length = length;
        }

        @Override
        public void run() {
            try {
                readFile.seek(startPosition);
                // read data according to the file size
                // write to file

                PreparedStatement ps = session
                        .prepare("insert into number(id, data) values(?, ?)");
                BatchStatement batch = new BatchStatement();
                for(int index = 0; index < length; index += READ_SIZE) {
                    byte[] bytes = new byte[READ_SIZE];
                    int len = readFile.read(bytes);
                    ByteBuffer byteBuffer = ByteBuffer.allocate(len);
                    byteBuffer.put(bytes);
                    BoundStatement bs = ps.bind(getIndex(), byteBuffer);
                    byteBuffer.clear();
                    batch.add(bs);
//                    if (index % 10 == 9) {
//                        session.execute(batch);
//                        batch.clear();
//                    }\
                    session.execute(batch);
                    batch.clear();
                }
                readFile.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
            super.run();
        }
    }
    private static int count = 0;
    synchronized int getIndex() {
        return count++;
    }
}
