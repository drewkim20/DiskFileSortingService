package externalsort;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * A package for Heap-sorting large binary files.
 *
 * @author Drew Kim drkim@calpoly.edu
 */
public class Driver {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                """
                Expected exactly 2 arguments. One for a file name to sort, and one for the number of buffers in
                the buffer pool.
                """
            );
        }
        String fileName = args[0];
        int numBuffers = Integer.parseInt(args[1]);
        RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
        BufferPoolService bps = new BufferPoolService(raf, numBuffers);
        HeapSortClient hsc = new HeapSortClient(bps);
        long startTime = System.currentTimeMillis();
        hsc.buildMaxHeap();
        hsc.sort();
        long endTime = System.currentTimeMillis();
        long sortTime = endTime - startTime;
        int recsPrinted = 0;
        for(int i = 0; i < raf.length(); i += 4096) {
            if(recsPrinted % 8 != 0)
                System.out.print("    ");
            raf.seek(i);
            System.out.print(raf.readShort() + " " + raf.readShort());
            recsPrinted++;
            if(recsPrinted % 8 == 0)
                System.out.println();
        }
        System.out.println();
        System.out.println("STATS");
        System.out.println("File name: " + fileName);
        System.out.println("Cache hits: " + bps.getCacheHits());
        System.out.println("Cache misses: " + bps.getCacheMisses());
        System.out.println("Disk reads: " + bps.getCacheMisses());
        System.out.println("Disk writes: " + bps.getDiskWrites());
        System.out.println("Time to sort: " + sortTime);
    }
}
