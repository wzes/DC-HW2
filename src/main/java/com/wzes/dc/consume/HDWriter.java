package com.wzes.dc.consume;

import com.wzes.dc.produce.Producer;
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
    public static final String PRODUCE_FILENAME = "hd_produce.dat";
    private static final String OUT_PATH = "hdfs://localhost:4000/user/user22/";

    private static int threadNumber = 1;

    private final int READ_SIZE = 1024 * 8;

    private final CountDownLatch countDownLatch = new CountDownLatch(threadNumber);

    public static void main(String[] args) throws IOException, InterruptedException {
        // Multi
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Thread num : " + threadNumber);
            long start = System.currentTimeMillis();

            // produce
            Producer producer = new Producer();
            if (i < 16) {
                producer.writeToFileByCompress(PRODUCE_FILENAME);
            }else {
                producer.writeByBufferedRandom(PRODUCE_FILENAME);
            }

            long proEnd = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (proEnd - start) + " ms");
            HDWriter hdWriter = new HDWriter();
            hdWriter.writeToHDFS();
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - proEnd) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            threadNumber *= 2;
        }
    }

    /**
     *
     * @return
     */
    private long getFileLength() {
        final File file = new File(PRODUCE_FILENAME);
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
        // get file system
        FileSystem fileSystem = FileSystem.get(URI.create(OUT_PATH), config, "user22");

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);

        Long totalSize = getFileLength();
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
            e.printStackTrace();
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
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
            super.run();
        }
    }
}
