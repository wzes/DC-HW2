package com.wzes.dc.consume;

import com.wzes.dc.bean.Task;
import com.wzes.dc.produce.Producer;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BufferedRandomAccessFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author Create by xuantang
 * @date on 11/19/17
 */
public class HDWriter {
    private static final String PRODUCE_FILENAME = "hd_produce.dat";
    private static final String OUT_PATH = "hdfs://localhost:4000/user/user22/";

    private static int threadNumber = 1;

    private final int READ_SIZE = 1024 * 8;

    private final CountDownLatch countDownLatch = new CountDownLatch(threadNumber);


    private static long middle = 0L;

    public static void main(String[] args) throws IOException, InterruptedException {
        // write result to file
        writeResult("hd_time.csv", "线程数,IO写入方法,时间/S\n", false);
        writeResult("hd_size.csv", "IO写入方法,文件空间大小/MB\n", false);

        // lz4 compress queue
        threadNumber = 1;
        middle = 0L;
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("First Way Thread num : " + threadNumber);
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
            HDWriter hdWriter = new HDWriter();
            hdWriter.writeToHDFSQueue();

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

            writeResult("hd_time.csv", threadNumber + "," + "LZ4 Compress Queue," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        System.out.println("    " + Thread.currentThread().toString() + " LZ4 Compress Queue: " +  getFileSize(PRODUCE_FILENAME) + " MB");
        writeResult("hd_size.csv", "LZ4 Compress Queue," + getFileSize(PRODUCE_FILENAME) + "\n", true);
        // snappy compress queue
        threadNumber = 1;
        middle = 0L;
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
                    producer.writeToFileByCompressQueue(PRODUCE_FILENAME, "SNAPPY");
                    middle = System.currentTimeMillis();
                    countDown.countDown();
                }
            });
            TaskQueue.getInstance().setProduceEnd(false);
            HDWriter hdWriter = new HDWriter();
            hdWriter.writeToHDFSQueue();

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

            writeResult("hd_time.csv", threadNumber + "," + "Snappy Compress Queue," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        writeResult("hd_size.csv", "Snappy Compress Queue," + getFileSize(PRODUCE_FILENAME) + "\n", true);
        System.out.println("    " + Thread.currentThread().toString() + " Snappy Compress Queue: " +  getFileSize(PRODUCE_FILENAME) + " MB");
        // normal
        threadNumber = 1;
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Third Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();

            Producer producer = new Producer();
            producer.writeToFileByCompress(PRODUCE_FILENAME, "LZ4");

            middle = System.currentTimeMillis();

            HDWriter hdWriter = new HDWriter();
            hdWriter.writeToHDFS();

            long end = System.currentTimeMillis();

            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (middle - start) + " ms");

            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - middle) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            writeResult("hd_time.csv", threadNumber + "," + "LZ4 Compress Normal," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        writeResult("hd_size.csv", "LZ4 Compress Normal," + getFileSize(PRODUCE_FILENAME) + "\n", true);
        System.out.println("    " + Thread.currentThread().toString() + " LZ4 Compress Queue: " +  getFileSize(PRODUCE_FILENAME) + " MB");
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

    public void writeToHDFS() throws IOException, InterruptedException {
        // get config
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        config.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        // get file system
        FileSystem fileSystem = FileSystem.get(URI.create(OUT_PATH), config, "user22");

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);

        Long totalSize = getFileLength(PRODUCE_FILENAME);
        Long sliceSize = totalSize / threadNumber;

        // calculate time
        for(int index = 0; index < threadNumber; index++) {
            BufferedRandomAccessFile readFile = null;
            String outPath = OUT_PATH + "hd" + index + ".dat";
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(outPath));
            try {
                readFile = new BufferedRandomAccessFile(PRODUCE_FILENAME, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            WriteToHDFSThread writeToHDFSThread = new WriteToHDFSThread(fsDataOutputStream
                , readFile, index * sliceSize, sliceSize);

            executorService.execute(writeToHDFSThread);
        }
        executorService.shutdown();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
        }
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
     * read and write thread
     */
    class WriteToHDFSThread extends Thread {
        FSDataOutputStream writeFile;
        BufferedRandomAccessFile readFile;
        Long startPosition;
        Long length;


        WriteToHDFSThread(FSDataOutputStream writeFile, BufferedRandomAccessFile readFile,
                             Long startPosition, Long length) {
            this.writeFile = writeFile;
            this.readFile = readFile;
            this.startPosition = startPosition;
            this.length = length;
        }

        @Override
        public void run() {
            try {
                readFile.seek(startPosition);

                for(int index = 0; index < length; index += READ_SIZE) {
                    byte[] bytes = new byte[READ_SIZE];
                    int len = readFile.read(bytes);
                    writeFile.write(bytes, 0, len);
                }
                readFile.close();
                writeFile.close();
            } catch (FileNotFoundException e) {
                // e.printStackTrace();
            } catch (IOException e) {
                // e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
            super.run();
        }
    }

    public void writeToHDFSQueue() throws IOException, InterruptedException {
        // get config
        Configuration config = new Configuration();

        config.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        config.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        // get file system
        FileSystem fileSystem = FileSystem.get(URI.create(OUT_PATH), config, "user22");

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);

        // calculate time
        for(int index = 0; index < threadNumber; index++) {
            BufferedRandomAccessFile readFile = null;
            String outPath = OUT_PATH + "hd" + index + ".dat";
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(outPath));
            try {
                readFile = new BufferedRandomAccessFile(PRODUCE_FILENAME, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            WriteToHDFSQueueThread writeToHDFSQueueThread = new WriteToHDFSQueueThread(fsDataOutputStream
                    , readFile);

            executorService.execute(writeToHDFSQueueThread);
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
    class WriteToHDFSQueueThread extends Thread {
        FSDataOutputStream writeFile;
        BufferedRandomAccessFile randomAccessFile;

        WriteToHDFSQueueThread(FSDataOutputStream writeFile, BufferedRandomAccessFile readFile) {
            this.writeFile = writeFile;
            this.randomAccessFile = readFile;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if(TaskQueue.getInstance().isProduceEnd()) {
                        break;
                    }
                    Task task = TaskQueue.getInstance().getTask();
                    if(task != null) {
                        Long start = task.getStart();
                        int length = task.getLength();
                        randomAccessFile.seek(start);
                        // read data
                        try {
                            // read data according to the file size
                            if(length >= READ_SIZE) {
                                // write to file
                                for(int index = 0; index < length; index += READ_SIZE) {
                                    int tmpLen;
                                    if (length - index < READ_SIZE) {
                                        tmpLen = length - index;
                                    } else {
                                        tmpLen = READ_SIZE;
                                    }
                                    byte[] bytes = new byte[tmpLen];
                                    int len = randomAccessFile.read(bytes);
                                    writeFile.write(bytes, 0, len);
                                }
                            }else {
                                byte[] bytes = new byte[length];
                                int len = randomAccessFile.read(bytes);
                                writeFile.write(bytes, 0, len);
                            }
                        } catch (Exception e) {
                            // System.out.println();
                        }
                    }
                }
                randomAccessFile.close();
                writeFile.close();
            } catch (Exception e) {
                // e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
            super.run();
        }
    }
}
