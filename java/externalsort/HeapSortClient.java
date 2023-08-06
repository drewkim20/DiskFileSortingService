package externalsort;

import java.io.IOException;

/**
 * The HeapSortClient class is the program attempting to sort the
 * disk file using the Heapsort algorithm.
 */
public class HeapSortClient {
    private final DiskFileService diskFileService;

    /**
     *
     * @param diskFileService the kind of service being used in order
     *                        to access the disk file
     */
    public HeapSortClient(DiskFileService diskFileService){
        this.diskFileService = diskFileService;
    }
    private void heapify(long length, long idx) throws IOException{
        long maxIdx = idx;
        Record root = diskFileService.read(idx);
        Record maxRecord = root;
        long leftIdx = 2 * idx + 1;
        long rightIdx = 2 * idx + 2;
        if(leftIdx < length){
            Record leftRecord = diskFileService.read(leftIdx);
            if(leftRecord.getKey() > maxRecord.getKey()){
                maxIdx = leftIdx;
                maxRecord = leftRecord;
            }
        }
        if(rightIdx < length){
            Record rightRecord = diskFileService.read(rightIdx);
            if(rightRecord.getKey() > maxRecord.getKey()){
                maxIdx = rightIdx;
                maxRecord = rightRecord;
            }
        }
        if(maxIdx != idx){
            diskFileService.swap(root, maxRecord);
            heapify(length, maxIdx);
        }
    }

    /**
     * The first step in the Heapsort algorithm which is to build a MaxHeap
     * (for ascending order sort).
     * @throws IOException for RandomAccessFile operations
     */
    public void buildMaxHeap() throws IOException{
        long length = diskFileService.length() / 4;
        long lastNonLeaf = length / 2 - 1;
        for(long i = lastNonLeaf; i >= 0 ; i--){
            heapify(length, i);
        }
    }

    /**
     * The second step in the Heapsort algorithm which is to repeatedly remove
     * the largest value and put it at the end of the list
     * @throws IOException for RandomAccessFile operations
     */
    public void sort() throws IOException{
        long lastIdx = (diskFileService.length() / 4) - 1;
        while(lastIdx > 0){
            Record first = diskFileService.read(0);
            Record last = diskFileService.read(lastIdx);
            diskFileService.swap(first, last);
            heapify(lastIdx, 0);
            lastIdx--;
        }
        diskFileService.flush();
    }
}
