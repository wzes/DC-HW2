package com.wzes.dc.consume;

import com.wzes.dc.bean.Task;
import com.wzes.dc.produce.Producer;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BufferedRandomAccessFile;

import java.io.*;
import java.util.concurrent.*;


import static com.wzes.dc.produce.Producer.*;

/**
 * @author Create by xuantang
 * @date on 11/6/17
 */
public class MTWriter {

    public static final String WRITE_FILENAME = "write.bin";
    private long length = 0;
    private static int threadNumber = 1;
    private final int READ_SIZE = 1024 * 8;
    private static final int INT_SIZE = 3;

    private final CountDownLatch countDownLatch = new CountDownLatch(threadNumber);
    public MTWriter() {

    }

    public MTWriter(int length, int threadNumber) {
        this.length = length;
        this.threadNumber = threadNumber;
    }

    public void readAndWriteData(String filename) {
        final File file = new File(PRODUCE_FILENAME);
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        final File endFile = new File(filename);
        if(!endFile.exists()) {
            try {
                endFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long s = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);
        // calculate time
        for(int index = 0; index < threadNumber; index++) {
            RandomAccessFile readFile = null;
            RandomAccessFile randomAccessFile = null;
            try {
                readFile = new RandomAccessFile(PRODUCE_FILENAME, "r");
                randomAccessFile = new RandomAccessFile(filename, "rw");
                randomAccessFile.setLength(length);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            ReadAndWriteThread readThread = new ReadAndWriteThread(randomAccessFile, readFile);
            executorService.execute(readThread);

        }
        executorService.shutdown();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        long e = System.currentTimeMillis();
        System.out.println(Thread.currentThread().toString() + " Total Time: " +  (e - s) + " ms");
    }



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

    public static void main(String[] args) {

        // 单线程池
//        ExecutorService singleThreadPool = Executors.newSingleThreadExecutor();
//
//        singleThreadPool.execute(new Runnable() {
//            @Override
//            public void run() {
//                Producer producer = new Producer();
//                producer.writeByBufferedOutput(PRODUCE_FILENAME);
//            }
//        });
//        singleThreadPool.shutdown();
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Thread num : " + threadNumber);
            long start = System.currentTimeMillis();
            Producer producer = new Producer();
            producer.writeByBufferedRandom(PRODUCE_FILENAME);
            long pro_end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (pro_end - start) + " ms");
            // System.out.println(Thread.currentThread().toString() + " Produce Time: " +  (e1 - s) + " ms");
            MTWriter mtWriter = new MTWriter();
            mtWriter.WriteAfterReadData(WRITE_FILENAME);
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - pro_end) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            threadNumber *= 2;
        }

//        try {
//            FileInputStream fileInputStream = new FileInputStream(new File("test"));
//            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
//            byte[] bytes = new byte[4];
//            int read;
//            Long index = 1L;
//            while(bufferedInputStream.read(bytes) != -1) {
//                if(index%256 == 0) {
//                    System.out.println(BytesUtils.byteArrayToInt(bytes));
//                }
//                index++;
//                if(index > 256* 100) {
//                    break;
//                }
//            }
//
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
    public void WriteAfterReadData(String filename) {

        final File file = new File(PRODUCE_FILENAME);
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        final File endFile = new File(filename);
        if(!endFile.exists()) {
            try {
                endFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Long totalSize = getFileLength();
        Long sliceSize = totalSize / threadNumber;

        //long s = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);
        // calculate time
        for(int index = 0; index < threadNumber; index++) {
            BufferedRandomAccessFile readFile = null;
            BufferedRandomAccessFile writeFile = null;
            try {
                readFile = new BufferedRandomAccessFile(PRODUCE_FILENAME, "r");
                writeFile = new BufferedRandomAccessFile(filename, "rw", 10);
                writeFile.setLength(totalSize);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            WriteAfterReadThread readThread = new WriteAfterReadThread(writeFile, readFile, index * sliceSize, sliceSize);
            executorService.execute(readThread);

        }
        executorService.shutdown();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //long e = System.currentTimeMillis();
        //System.out.println(Thread.currentThread().toString() + " Write over: " +  (e - s) + " ms");
    }

    /**
     * read and write thread
     */
    class ReadAndWriteThread extends Thread {
        RandomAccessFile endFile;
        RandomAccessFile randomAccessFile;

        ReadAndWriteThread(RandomAccessFile endFile, RandomAccessFile randomAccessFile) {
            this.endFile = endFile;
            this.randomAccessFile = randomAccessFile;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if(TaskQueue.getInstance().isQueueEmpty() && TaskQueue.getInstance().isProduceEnd()) {
                        break;
                    }
                    Task task = TaskQueue.getInstance().getTask();
                    if(task != null) {
                        Long start = task.getStart();
                        int length = task.getLength();
                        randomAccessFile.seek(start);
                        // read data
                        endFile.setLength(getFileLength());
                        endFile.seek(start);
                        // read data according to the file size
                        if(length >= READ_SIZE) {
                            // write to file
                            for(int index = 0; index < length; index += READ_SIZE) {
                                byte[] bytes = new byte[READ_SIZE];
                                randomAccessFile.read(bytes);
                                endFile.write(bytes);
                            }
                        }else {
                            byte[] bytes = new byte[length];
                            randomAccessFile.read(bytes);
                            endFile.write(bytes);
                        }
                    }
                }
                randomAccessFile.close();
                endFile.close();
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


    /**
     * read and write thread
     */
    class WriteAfterReadThread extends Thread {
        BufferedRandomAccessFile writeFile;
        BufferedRandomAccessFile readFile;
        Long startPosition;
        Long length;


        WriteAfterReadThread(BufferedRandomAccessFile writeFile, BufferedRandomAccessFile readFile,
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
                // read data
                writeFile.seek(startPosition);
                // read data according to the file size
                // write to file
                for(int index = 0; index < length; index += READ_SIZE) {
                    byte[] bytes = new byte[READ_SIZE];
                    readFile.read(bytes);
                    writeFile.write(bytes);
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
