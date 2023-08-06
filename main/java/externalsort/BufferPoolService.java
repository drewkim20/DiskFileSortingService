package externalsort;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * The BufferPoolService class serves as the mediator between the
 * Heapsort algorithm and interaction with the Random Access File.
 * Implements the Proxy design pattern.
 */
public class BufferPoolService implements DiskFileService{
    private final RandomAccessFile raf;
    private final int numBuffers;
    private final LinkedList<BufferNode> bufferPool = new LinkedList<>();
    private int cacheHits = 0;
    private int cacheMisses = 0;
    private int diskWrites = 0;
    private static final int RECORDSPERBLOCK = 1024;

    /**
     * BufferPoolService defined by the RandomAccessFile raf it must access
     * and the number of buffers numBuffers allowed in its buffer pool. Also
     * contains a buffer pool as a list of BufferNodes, ints cacheHits,
     * cacheMisses, and diskWrites to keep track of stats, and the constant
     * variable RECORDSPERBLOCK.
     * @param raf
     * @param numBuffers
     */
    public BufferPoolService(RandomAccessFile raf, int numBuffers){
        this.raf = raf;
        this.numBuffers = numBuffers;
    }
    public int getCacheHits(){
        return cacheHits;
    }
    public int getCacheMisses(){
        return cacheMisses;
    }
    public int getDiskWrites(){
        return diskWrites;
    }

    /**
     * Grabs a Record from the buffer pool if present (faster access)
     * or reads from disk file. Updates buffer pool using the Least Recently
     * Used method.
     * @param heapIdx the index that the Heapsort algorithm is
     *                requesting
     * @return the requested Record at the given offset
     * @throws IOException for RandomAccessFile operations
     */
    @Override
    public Record read(long heapIdx) throws IOException{
        Record r;
        long blockNum = heapIdx / RECORDSPERBLOCK;
        int blockIdx = (int)(heapIdx % RECORDSPERBLOCK);
        int bpIdx = find(blockNum);
        if(bpIdx != -1){
            BufferNode buffNode = bufferPool.get(bpIdx);
            List<Record> block = buffNode.getBlock();
            r = block.get(blockIdx);
            bufferPool.remove(bpIdx);
            bufferPool.add(0, buffNode);
        } else{
            long offset = heapIdx * 4;
            raf.seek(offset);
            short key = raf.readShort();
            short value = raf.readShort();
            r = new Record(offset, key, value);
            cacheMissRead(blockNum);
            resizePool();
        }
        return r;
    }

    /**
     * Writes the BufferNode representing a block to the disk file
     * @param buffNode the BufferNode to be written
     * @throws IOException for RandomAccessFile operations
     */
    @Override
    public void write(BufferNode buffNode) throws IOException{
        diskWrites++;
        long offset = buffNode.getBlockNum() * RECORDSPERBLOCK * 4;
        List<Record> block = buffNode.getBlock();
        ByteBuffer bb = ByteBuffer.allocate(RECORDSPERBLOCK * 4);
        for(int i = 0; i < RECORDSPERBLOCK; i++){
            bb.putShort(i * 4, block.get(i).getKey());
            bb.putShort(i * 4 + 2, block.get(i).getValue());
        }
        byte[] byteArr = bb.array();
        raf.seek(offset);
        raf.write(byteArr);
    }

    /**
     * Swaps the two given records. Checks the buffer pool first for
     * the records, then reads from the disk file if necessary. Updates the
     * buffer pool after the swap.
     * @param r1 the first record to be swapped
     * @param r2 the second record to be swapped
     * @throws IOException for RandomAccessFile operations
     */
    @Override
    public void swap(Record r1, Record r2) throws IOException{
        long blockNum1 = (r1.getOffset() / 4) / RECORDSPERBLOCK;
        long blockNum2 = (r2.getOffset() / 4) / RECORDSPERBLOCK;
        int blockOffset1 = (int)((r1.getOffset() / 4) % RECORDSPERBLOCK);
        int blockOffset2 = (int)((r2.getOffset() / 4) % RECORDSPERBLOCK);
        int bpIdx1 = find(blockNum1);
        long temp = r2.getOffset();
        if(bpIdx1 == -1){
            cacheMisses++;
            cacheMissRead(blockNum1);
            bpIdx1 = 0;
        }
        BufferNode buffNode1 = bufferPool.get(bpIdx1);
        if (!buffNode1.getMustWrite())
            buffNode1.setMustWrite(true);
        List<Record> block1 = buffNode1.getBlock();
        block1.remove(blockOffset1);
        r2.setOffset(r1.getOffset());
        block1.add(blockOffset1, r2);
        buffNode1.setBlock(block1);
        bufferPool.remove(bpIdx1);
        bufferPool.add(bpIdx1, buffNode1);
        int bpIdx2 = find(blockNum2);
        if(bpIdx2 == -1) {
            cacheMissRead(blockNum2);
            bpIdx2 = 0;
        }
        BufferNode buffNode2 = bufferPool.get(bpIdx2);
        if (!buffNode2.getMustWrite())
            buffNode2.setMustWrite(true);
        List<Record> block2 = buffNode2.getBlock();
        block2.remove(blockOffset2);
        r1.setOffset(temp);
        block2.add(blockOffset2, r1);
        buffNode2.setBlock(block2);
        bufferPool.remove(bpIdx2);
        bufferPool.add(bpIdx2, buffNode2);
        resizePool();
    }

    /**
     * Used to determine indexing
     * @return the length of the RandomAccessFile
     * @throws IOException for RandomAccessFile operations
     */
    @Override
    public long length() throws IOException {
        return raf.length();
    }

    /**
     * The buffer pool must be flushed after the Heapsort algorithm is
     * finished. This is to ensure all blocks are being written to the
     * random access file
     * @throws IOException for RandomAccessFile operations
     */
    @Override
    public void flush() throws IOException{
        while(!bufferPool.isEmpty()){
            BufferNode last = bufferPool.getLast();
            if(last.getMustWrite())
                write(last);
            bufferPool.removeLast();
        }
    }
    private int find(long blockNum){
        for(int i = 0; i < bufferPool.size(); i++){
            if(blockNum == bufferPool.get(i).getBlockNum()){
                cacheHits++;
                return i;
            }
        }
        cacheMisses++;
        return -1;
    }
    private void cacheMissRead(long blockNum) throws IOException{
        byte[] byteArr = new byte[RECORDSPERBLOCK * 4];
        raf.seek(blockNum * RECORDSPERBLOCK * 4);
        raf.read(byteArr);
        ByteBuffer bb = ByteBuffer.wrap(byteArr);
        List<Record> block = new ArrayList<>();
        for(int i = 0; i < RECORDSPERBLOCK; i++){
            long recOffset = (blockNum * RECORDSPERBLOCK + i) * 4;
            block.add(new Record(recOffset, bb.getShort(i * 4), bb.getShort(i * 4 + 2)));
        }
        bufferPool.add(0, new BufferNode(blockNum, block));
    }
    private void resizePool() throws IOException{
        while(bufferPool.size() > numBuffers){
            BufferNode last = bufferPool.getLast();
            if(last.getMustWrite())
                write(last);
            bufferPool.removeLast();
        }
    }
}
