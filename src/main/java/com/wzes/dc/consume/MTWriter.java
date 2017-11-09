package com.wzes.dc.consume;

import com.wzes.dc.bean.Task;
import com.wzes.dc.produce.Producer;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BytesUtils;

import java.io.*;
import java.util.concurrent.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static com.wzes.dc.produce.Producer.*;

/**
 * @author Create by xuantang
 * @date on 11/6/17
 */
public class MTWriter {

    public static final String WRITE_FILENAME = "write.bin";
    private long length = 0;
    private int threadNumber = 4;

    public MTWriter() {

    }

    public MTWriter(int length, int threadNumber) {
        this.length = length;
        this.threadNumber = threadNumber;
    }

    public void writeData(String filename) {
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
            ReadThread readThread = new ReadThread(randomAccessFile, readFile);
            readThread.start();
        }
    }

    public long getFileLength() {
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
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                Producer producer = new Producer();
//                producer.writeToFile(PRODUCE_FILENAME);
//            }
//        }).start();

        ThreadFactory namedThreadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return null;
            }
        };

        ExecutorService singleThreadPool = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

        singleThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Producer producer = new Producer();
                producer.writeToFile(PRODUCE_FILENAME);
            }
        });
        singleThreadPool.shutdown();


        MTWriter mtWriter = new MTWriter();
        mtWriter.writeData(WRITE_FILENAME);

//        try {
//            FileInputStream fileInputStream = new FileInputStream(new File(PRODUCE_FILENAME));
//            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
//            byte[] bytes = new byte[4];
//            int read;
//            while(bufferedInputStream.read(bytes) != -1) {
//                System.out.println(BytesUtils.byteArrayToInt(bytes));
//            }
//
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


    }


    class ReadThread extends Thread {
        RandomAccessFile endFile;
        RandomAccessFile randomAccessFile;
        Long start;
        Long end;

        ReadThread(RandomAccessFile endFile, RandomAccessFile randomAccessFile) {
            this.endFile = endFile;
            this.randomAccessFile = randomAccessFile;
        }

        ReadThread(RandomAccessFile endFile, RandomAccessFile randomAccessFile, Long start, Long end) {
            this.endFile = endFile;
            this.randomAccessFile = randomAccessFile;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            try {
                long s = System.currentTimeMillis();
                while (true) {
                    if(TaskQueue.getInstance().isQueueEmpty() && TaskQueue.getInstance().isProduceEnd()) {
                        break;
                    }
                    Task task = TaskQueue.getInstance().getTask();
                    if(task != null) {
                        Long start = task.getStart();
                        int length = task.getLength();
                        // System.out.println(start + "   " + length);
                        randomAccessFile.seek(start);
                        // read data
                        byte[] bytes = new byte[length];
                        randomAccessFile.read(bytes);
                        // write to file
                        endFile.setLength(getFileLength());
                        endFile.seek(start);
                        endFile.write(bytes);
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
//                System.out.println(end - start);

//                for(int index = 0; index < end - start; index += 256*4 ) {
//                    Long read = randomAccessFile.length();
//                    System.out.println(read);

                    // System.out.println(BytesUtils.byteArrayToInt(bytes));
                    // System.out.println(bytes.length);
//                }
                long e = System.currentTimeMillis();
                randomAccessFile.close();
                endFile.close();
                System.out.println(Thread.currentThread().toString() + " Total Time: " +  (e - s) + " ms");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            super.run();
        }
    }
}
