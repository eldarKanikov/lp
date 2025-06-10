package com.lightspeed.ip_addr_counter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLongArray;

public class ParallelFileReader {
    private static final int BITS = 32;
    private static final int ARRAY_SIZE = 1 << (BITS - 6);
    public static void main(String[] args) throws Exception {
        String filePath = "/Users/eldarkanikov/Downloads/ip_addresses";
        int numThreads = 30;

        int blockSize = 16 * 1024; // 16 KB
        AtomicLongArray positive = new AtomicLongArray(ARRAY_SIZE);
        AtomicLongArray negative = new AtomicLongArray(ARRAY_SIZE);

        long startTime = System.currentTimeMillis();
        long[] boundaries = calculateChunkBoundaries(filePath, numThreads);

        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            long start = boundaries[i];
            long end = boundaries[i + 1];
            Thread.ofVirtual().start(() -> {
                try {
                    processBlockRange(filePath, start, end, blockSize, positive, negative);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                latch.countDown();
            });
        }

        latch.await();
        long uniqueCount = countBits(positive) + countBits(negative);
        long endTime = System.currentTimeMillis();
        System.out.println("Время выполнения: " + ((endTime - startTime) / 1000) + " сек");
        System.out.println("Уникальных IP: " + uniqueCount);
        System.out.println("Число потоков " + numThreads);
    }

    public static long[] calculateChunkBoundaries(String filePath, int numChunks) throws IOException {
        File file = new File(filePath);
        long fileSize = file.length();
        long[] boundaries = new long[numChunks + 1];
        boundaries[0] = 0;
        boundaries[numChunks] = fileSize;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            for (int i = 1; i < numChunks; i++) {
                long approx = (fileSize * i) / numChunks;
                raf.seek(approx);
                int b;
                while ((b = raf.read()) != -1) {
                    if (b == '\n') {
                        boundaries[i] = raf.getFilePointer();
                        break;
                    }
                }
                if (boundaries[i] == 0) boundaries[i] = fileSize;
            }
        }
        return boundaries;
    }

    public static void processBlockRange(String filePath, long start, long end, int blockSize,
                                         AtomicLongArray positive, AtomicLongArray negative) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r");
             FileChannel channel = raf.getChannel()) {

            raf.seek(start);
            long position = start;
            StringBuilder lineBuffer = new StringBuilder();

            while (position < end) {
                int bytesToRead = (int) Math.min(blockSize, end - position);
                ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);
                channel.read(buffer, position);
                buffer.flip();

                for (int i = 0; i < buffer.limit(); i++) {
                    byte b = buffer.get();
                    if (b == '\n') {
                        String line = lineBuffer.toString();
                        handleLine(line, positive, negative);
                        lineBuffer.setLength(0);
                    } else if (b != '\r') {
                        lineBuffer.append((char) b);
                    }
                }
                position += bytesToRead;
            }
            if (!lineBuffer.isEmpty()) {
                String line = lineBuffer.toString();
                handleLine(line, positive, negative);
            }
        }
    }

    private static void handleLine(String line, AtomicLongArray positive, AtomicLongArray negative) {
        int ipAsInt = ipToInt(line);
        int idx, bit;
        if (ipAsInt >= 0) {
            idx = ipAsInt >>> 6;
            bit = ipAsInt & 63;
            positive.getAndUpdate(idx, l -> l | (1L << bit));
        } else {
            int negIdx = (-(ipAsInt + 1)) >>> 6;
            int negBit = (-(ipAsInt + 1)) & 63;
            negative.getAndUpdate(negIdx, l -> l | (1L << negBit));
        }
    }

    private static long countBits(AtomicLongArray arr) {
        long count = 0;
        for (int i = 0; i < arr.length(); i++) {
            count += Long.bitCount(arr.get(i));
        }
        return count;
    }
    public static int ipToInt(String ip) {
        int result = 0;
        int num = 0;
        for (int i = 0; i < ip.length(); i++) {
            char symbol = ip.charAt(i);
            if (symbol == '.') {
                result = result << 8 | num;
                num = 0;
            } else {
                num = num * 10 + (symbol - '0');
            }
        }
        return result << 8 | num;
    }
}