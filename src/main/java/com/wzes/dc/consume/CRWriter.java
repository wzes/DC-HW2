package com.wzes.dc.consume;

import com.datastax.driver.core.*;
import com.wzes.dc.bean.Task;
import com.wzes.dc.produce.Producer;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BufferedRandomAccessFile;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author Create by xuantang
 * @date on 11/6/17
 */
public class CRWriter {
    private static final String PRODUCE_FILENAME = "cr_produce.dat";
    private static int threadNumber = 1;

    private final int READ_SIZE = 1024 * 128;

    private static Cluster cluster = null;
    private static Session session = null;
    private static long middle = 0L;
    private final CountDownLatch countDownLatch = new CountDownLatch(threadNumber);

    public static void main(String[] args) throws IOException, InterruptedException {

        // write result to file
        writeResult("cr_time.csv", "线程数,IO写入方法, 时间/S\n", false);
        writeResult("cr_size.csv", "IO写入方法,文件空间大小/MB\n", false);

        cluster = Cluster.builder()
                .addContactPoint("148.100.92.158")
                .withPort(4392)
                .withClusterName("tongji01")
                .withCredentials("user22", "1552730")
                .build();
        session = cluster.connect("keyspace_user22");
        // MultiThread
        threadNumber = 1;
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("First Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();
            // produce
            Producer producer = new Producer();
            producer.writeToFileByCompress(PRODUCE_FILENAME, "LZ4");
            long proEnd = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (proEnd - start) + " ms");
            CRWriter crWriter = new CRWriter();
            crWriter.writeToCRNormal(session);
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - proEnd) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");
            writeResult("cr_time.csv", threadNumber + "," + "LZ4 Compress Normal," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        writeResult("cr_size.csv", "LZ4 Compress Normal," + getFileSize(PRODUCE_FILENAME) + "\n", true);
        System.out.println("    " + Thread.currentThread().toString() + " LZ4 Compress Normal: " +  getFileSize(PRODUCE_FILENAME) + " MB");
        // session = null;
        threadNumber = 1;
        // queue insert
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Second Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();

            ExecutorService executorService = Executors.newFixedThreadPool(1);
            final CountDownLatch countDown = new CountDownLatch(1);
            // create new thread
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    // produce
                    Producer producer = new Producer();
                    producer.writeToFileByCompressQueue(PRODUCE_FILENAME, "LZ4");
                    middle = System.currentTimeMillis();
                    countDown.countDown();
                }
            });
            TaskQueue.getInstance().setProduceEnd(false);
            CRWriter crWriter = new CRWriter();
            crWriter.writeToCRQueue(session);

            executorService.shutdown();
            try {
                countDown.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (middle - start) + " ms");
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - middle) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            writeResult("cr_time.csv", threadNumber + "," + "LZ4 Compress Queue," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        writeResult("cr_size.csv", "LZ4 Compress Queue," + getFileSize(PRODUCE_FILENAME) + "\n", true);
        System.out.println("    " + Thread.currentThread().toString() + " LZ4 Compress Queue: " +  getFileSize(PRODUCE_FILENAME) + " MB");
        // Snappy Compress
        threadNumber = 1;
        middle = 0L;
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Third Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();

            ExecutorService executorService = Executors.newFixedThreadPool(1);
            final CountDownLatch countDown = new CountDownLatch(1);
            // create new thread
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    // produce
                    Producer producer = new Producer();
                    producer.writeToFileByCompressQueue(PRODUCE_FILENAME, "SNAPPY");
                    middle = System.currentTimeMillis();
                    countDown.countDown();
                }
            });
            TaskQueue.getInstance().setProduceEnd(false);
            CRWriter crWriter = new CRWriter();
            crWriter.writeToCRQueue(session);

            executorService.shutdown();
            try {
                countDown.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (middle - start) + " ms");
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - middle) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            writeResult("cr_time.csv", threadNumber + "," + "Snappy Compress Queue," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        writeResult("cr_size.csv", "Snappy Compress Queue," + getFileSize(PRODUCE_FILENAME) + "\n", true);
        System.out.println("    " + Thread.currentThread().toString() + " Snappy Compress Queue: " +  getFileSize(PRODUCE_FILENAME) + " MB");
        session.close();
        cluster.close();
    }

    /**
     * Write result to file
     * @param filename
     * @param res
     */
    public static void writeResult(String filename, String res, boolean append) {
        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(file, append);
            fileWriter.append(res);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param filename
     * @return
     */
    public static double getFileSize(String filename) {
        final File file = new File(filename);
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return file.length() / 1024.0 / 1024.0;
    }

    /**
     *
     * @param filename
     * @return
     */
    public static long getFileLength(String filename) {
        final File file = new File(filename);
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return file.length();
    }

    /**
     * Normal write
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeToCRNormal(Session session) throws IOException, InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);

        int totalSize = (int) getFileLength(PRODUCE_FILENAME);
        int sliceSize = totalSize / threadNumber;

        // calculate time
        for(int index = 0; index < threadNumber; index++) {
            BufferedRandomAccessFile readFile = null;

            try {
                readFile = new BufferedRandomAccessFile(PRODUCE_FILENAME, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            WriteToCRNormalThread writeToCRThread = new WriteToCRNormalThread(session, readFile,
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
     * Normal write
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeToCRQueue(Session session) throws IOException, InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);
        // calculate time
        for(int index = 0; index < threadNumber; index++) {

            WriteToCRQueueThread writeToCRThread = new WriteToCRQueueThread(session, PRODUCE_FILENAME);

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
    class WriteToCRNormalThread extends Thread {
        Session session;
        BufferedRandomAccessFile readFile;
        int startPosition;
        int length;
        WriteToCRNormalThread(Session session, BufferedRandomAccessFile readFile,
                             int startPosition, int length) {
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
                int i = 1;
                for(int index = 0; index < length; index += READ_SIZE) {
                    // get true len
                    int tmpLen;
                    if (length - index < READ_SIZE) {
                        tmpLen = length - index;
                    } else {
                        tmpLen = READ_SIZE;
                    }
                    byte[] bytes = new byte[tmpLen];
                    int len = readFile.read(bytes);
                    if (len != -1) {
                        ByteBuffer byteBuffer = ByteBuffer.allocate(len);
                        byteBuffer.put(bytes);
                        BoundStatement bs = ps.bind(getIndex(), byteBuffer);
                        byteBuffer.clear();
                        batch.add(bs);
                    }

                    if (i % 10 == 0) {
                        session.execute(batch);
                        batch.clear();
                    }
                    i++;
                }
                session.execute(batch);
                batch.clear();
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
    private synchronized int getIndex() {
        return count++;
    }

    /**
     * read and write thread
     */
    class WriteToCRQueueThread extends Thread {
        Session session;
        String filename;
        BufferedRandomAccessFile readFile;

        WriteToCRQueueThread(Session session, String filename) {
            this.session = session;
            this.filename = filename;
        }

        private int len;
        @Override
        public void run() {
            try {
                while (true) {
                    if(TaskQueue.getInstance().isProduceEnd()) {
                        break;
                    }
                    Task task = TaskQueue.getInstance().getTask();
                    if(task != null) {
                        if (readFile == null) {
                            readFile = new BufferedRandomAccessFile(filename, "r");
                        }
                        Long start = task.getStart();
                        int length = task.getLength();
                        readFile.seek(start);
                        // read data
                        PreparedStatement ps = session
                                .prepare("insert into number(id, data) values(?, ?)");
                        BatchStatement batch = new BatchStatement();
                        int i = 1;
                        for(int index = 0; index < length; index += READ_SIZE) {
                            // get true len
                            int tmpLen;
                            if (length - index < READ_SIZE) {
                                tmpLen = length - index;
                            } else {
                                tmpLen = READ_SIZE;
                            }
                            byte[] bytes = new byte[tmpLen];
                            //System.out.println(tmpLen);
                            try {
                                len = readFile.read(bytes, 0, tmpLen);
                            } catch (Exception e) {
                                //System.out.println(start + "+++++++++++++++++++" + tmpLen);
                            }
                            if (len != -1) {
                                ByteBuffer byteBuffer = ByteBuffer.allocate(len);
                                byteBuffer.put(bytes, 0, len);
                                BoundStatement bs = ps.bind(getIndex(), byteBuffer);
                                batch.add(bs);
                                byteBuffer.clear();
                            }
                            // write to cassandra
                            if (i % 10 == 0) {
                                session.execute(batch);
                                batch.clear();
                            }
                            i++;
                        }
                        session.execute(batch);
                        batch.clear();
                    }
                }
                if (readFile != null) {
                    readFile.close();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
            super.run();
        }
    }
}
