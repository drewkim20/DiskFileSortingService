package test;

import externalsort.*;
import externalsort.Record;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import static org.junit.Assert.*;

public class TestHeap {
    private String fileName = "testFile.txt";
    @Test
    public void testBufferPoolRead() throws IOException {
        Utils.generateByteFile(1024, fileName);
        RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
        BufferPoolService bsp = new BufferPoolService(raf, 1);
        long heapIdx = 500;
        Record r = bsp.read(heapIdx);
        long offset = heapIdx * 4;
        raf.seek(offset);
        short key = raf.readShort();
        short value = raf.readShort();
        Record expected = new Record(offset, key, value);
        assertEquals(expected, r);
    }
    @Test
    public void testBufferPoolWrite() throws IOException{
        Utils.generateByteFile(2048, fileName);
        RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
        BufferPoolService bsp = new BufferPoolService(raf, 1);
        long heapIdx1 = 1023;
        long heapIdx2 = 1024;
        Record r1 = bsp.read(heapIdx1);
        raf.seek(heapIdx2 * 4);
        Record r2 = new Record(heapIdx2 * 4, raf.readShort(), raf.readShort());
        bsp.swap(r1, r2);
        raf.seek(r1.getOffset());
        assertEquals(raf.readShort(), r2.getKey());
        assertEquals(bsp.getDiskWrites(), 1);
    }
    @Test
    public void testBufferPoolSwap() throws IOException{
        Utils.generateByteFile(4096, fileName);
        RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
        BufferPoolService bsp = new BufferPoolService(raf, 2);
        long heapIdx1 = 2047;
        long heapIdx2 = 2048;
        long heapIdx3 = 0;
        long heapIdx4 = 3072;
        Record r1 = bsp.read(heapIdx1);
        Record r2 = bsp.read(heapIdx2);
        bsp.swap(r1, r2);
        bsp.read(heapIdx3);
        bsp.read(heapIdx4);
        raf.seek(heapIdx1 * 4);
        assertEquals(raf.readShort(), r2.getKey());
        raf.seek(heapIdx2 * 4);
        assertEquals(raf.readShort(), r1.getKey());
        assertEquals(bsp.getCacheMisses(), 4);
        assertEquals(bsp.getCacheHits(), 2);
    }
    @Test
    public void testFlush() throws IOException{
        Utils.generateByteFile(4096, fileName);
        RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
        BufferPoolService bsp = new BufferPoolService(raf, 2);
        bsp.read(0);
        bsp.read(1024);
        raf.seek(0);
        Record r1 = new Record(0, raf.readShort(), raf.readShort());
        raf.seek(2048);
        Record r2 = new Record(2048, raf.readShort(), raf.readShort());
        bsp.swap(r1, r2);
        bsp.flush();
        raf.seek(0);
        short key1 = raf.readShort();
        raf.seek(2048);
        short key2 = raf.readShort();
        assertEquals(key1, r2.getKey());
        assertEquals(key2, r1.getKey());
    }
    @Test
    public void testHeapSort() throws IOException{
        Utils.generateByteFile(4096, fileName);
        RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
        BufferPoolService bsp = new BufferPoolService(raf, 2);
        HeapSortClient hsc = new HeapSortClient(bsp);
        hsc.buildMaxHeap();
        hsc.sort();
        assertTrue(Utils.checkFile(fileName));
    }
    @Test
    public void testRecord(){
        short key = 4;
        short value = 5;
        Record r1 = new Record(2048, key, value);
        Record r2 = new Record(2048, key, value);
        Record r3 = new Record(0, key, value);
        Record r4 = new Record(2048, (short)(key - 1), value);
        Record r5 = new Record(2048, key, (short)(value - 1));
        Integer n = 2048;
        assertEquals(r1, r2);
        assertNotEquals(r1, r3);
        assertNotEquals(r1, r4);
        assertNotEquals(r1, r5);
        assertNotEquals(r1, n);
        assertEquals(r3.hashCode(), 32);
    }
}
