package com.wzes.dc.consume;

import com.wzes.dc.bean.Task;
import com.wzes.dc.produce.Producer;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BufferedRandomAccessFile;

import java.io.*;
import java.util.concurrent.*;


/**
 * @author Create by xuantang
 * @date on 11/6/17
 */
public class MTWriter {
    private static final String PRODUCE_FILENAME = "mt_produce.dat";
    private static final String WRITE_FILENAME = "mt_write.dat";

    private long length = 0;
    private static int threadNumber = 1;
    private final int READ_SIZE = 1024 * 8;

    private final CountDownLatch countDownLatch = new CountDownLatch(threadNumber);
    public MTWriter() {

    }


    private static long middle = 0L;

    public static void main(String[] args) {
        // write result to file
        writeResult("mt_time.csv", "线程数,IO写入方法,时间/S\n", false);
        writeResult("mt_size.csv", "IO写入方法,文件空间大小/MB\n", false);
        // way first
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("First Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();
            Producer producer = new Producer();
            producer.writeByBufferedRandom(PRODUCE_FILENAME);
            long proEnd = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (proEnd - start) + " ms");
            MTWriter mtWriter = new MTWriter();
            mtWriter.WriteAfterReadData(WRITE_FILENAME);
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - proEnd) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            writeResult("mt_time.csv", threadNumber + "," + "BufferedRandom," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        System.out.println("    " + Thread.currentThread().toString() + " Total size: " +  getFileSize(WRITE_FILENAME) + " MB");
        writeResult("mt_size.csv", "BufferedRandom," + getFileSize(WRITE_FILENAME) + "\n", true);
        // second way
        threadNumber = 1;
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Second Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();


            ExecutorService executorService = Executors.newFixedThreadPool(1);
            final CountDownLatch countDown = new CountDownLatch(1);

            // create new thread
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    Producer producer = new Producer();
                    producer.writeToFileByAnotherQueue(PRODUCE_FILENAME);
                    middle = System.currentTimeMillis();
                    countDown.countDown();
                }
            });
            TaskQueue.getInstance().setProduceEnd(false);
            MTWriter mtWriter = new MTWriter();
            mtWriter.WriteAndReadData(WRITE_FILENAME);

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

            writeResult("mt_time.csv", threadNumber + "," + "Queue," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        System.out.println("    " + Thread.currentThread().toString() + " Total size: " +  getFileSize(WRITE_FILENAME) + " MB");
        writeResult("mt_size.csv", "Queue," + getFileSize(WRITE_FILENAME) + "\n", true);

        // third way
        threadNumber = 1;
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Third Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();
            Producer producer = new Producer();
            producer.writeToFileByCompress(PRODUCE_FILENAME, "LZ4");
            long proEnd = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (proEnd - start) + " ms");
            MTWriter mtWriter = new MTWriter();
            mtWriter.WriteAfterReadData(WRITE_FILENAME);
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - proEnd) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            writeResult("mt_time.csv", threadNumber + "," + "LZ4 Compress," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        System.out.println("    " + Thread.currentThread().toString() + " Total size: " +  getFileSize(WRITE_FILENAME) + " MB");
        writeResult("mt_size.csv", "LZ4 Compress," + getFileSize(WRITE_FILENAME) + "\n", true);

        // fourth way
        threadNumber = 1;
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Third Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();
            Producer producer = new Producer();
            producer.writeToFileByCompress(PRODUCE_FILENAME, "SNAPPY");
            long proEnd = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (proEnd - start) + " ms");
            MTWriter mtWriter = new MTWriter();
            mtWriter.WriteAfterReadData(WRITE_FILENAME);
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - proEnd) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            writeResult("mt_time.csv", threadNumber + "," + "Snappy Compress," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        System.out.println("    " + Thread.currentThread().toString() + " Total size: " +  getFileSize(WRITE_FILENAME) + " MB");
        writeResult("mt_size.csv", "Snappy Compress," + getFileSize(WRITE_FILENAME) + "\n", true);

        // fifth way
        threadNumber = 1;
        for (int i = 1; i <= 32; i *= 2 ) {
            System.out.println("Third Way Thread num : " + threadNumber);
            long start = System.currentTimeMillis();
            Producer producer = new Producer();
            producer.writeToFileByCompress(PRODUCE_FILENAME, "GZIP");
            long proEnd = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Produce over: " +  (proEnd - start) + " ms");
            MTWriter mtWriter = new MTWriter();
            mtWriter.WriteAfterReadData(WRITE_FILENAME);
            long end = System.currentTimeMillis();
            System.out.println("    " + Thread.currentThread().toString() + " Write over: " +  (end - proEnd) + " ms");
            System.out.println("    " + Thread.currentThread().toString() + " Total Time: " +  (end - start) + " ms");

            writeResult("mt_time.csv", threadNumber + "," + "Gzip Compress," + (end - start) / 1000.0 + "\n", true);
            threadNumber *= 2;
        }
        System.out.println("    " + Thread.currentThread().toString() + " Total size: " +  getFileSize(WRITE_FILENAME) + " MB");
        writeResult("mt_size.csv", "Gzip Compress," + getFileSize(WRITE_FILENAME) + "\n", true);
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
     */
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
            BufferedRandomAccessFile readFile = null;
            BufferedRandomAccessFile randomAccessFile = null;
            try {
                readFile = new BufferedRandomAccessFile(PRODUCE_FILENAME, "r");
                randomAccessFile = new BufferedRandomAccessFile(filename, "rw");
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


    /**
     * Get the length of file
     * @return length
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
     */
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
    }

    /**
     *
     * @param filename
     */
    public void WriteAndReadData(String filename) {
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
            ReadAndWriteThread readThread = new ReadAndWriteThread(writeFile, readFile);
            executorService.execute(readThread);

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
    class ReadAndWriteThread extends Thread {
        BufferedRandomAccessFile endFile;
        BufferedRandomAccessFile randomAccessFile;

        ReadAndWriteThread(BufferedRandomAccessFile endFile, BufferedRandomAccessFile randomAccessFile) {
            this.endFile = endFile;
            this.randomAccessFile = randomAccessFile;
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
                        endFile.setLength(getFileLength());
                        endFile.seek(start);
                        // read data according to the file size
                        if(length >= READ_SIZE) {
                            // write to file
                            for(int index = 0; index < length; index += READ_SIZE) {
                                byte[] bytes = new byte[READ_SIZE];
                                int len = randomAccessFile.read(bytes);
                                endFile.write(bytes, 0, len);
                            }
                        }else {
                            byte[] bytes = new byte[length];
                            int len = randomAccessFile.read(bytes);
                            endFile.write(bytes, 0, len);
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
    class NormalWriteAfterReadThread extends Thread {
        RandomAccessFile writeFile;
        RandomAccessFile readFile;
        Long startPosition;
        Long length;


        NormalWriteAfterReadThread(RandomAccessFile writeFile, RandomAccessFile readFile,
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
