package externalsort;

import java.util.List;

/**
 * The BufferNode class represents an element of the buffer pool. It
 * contains the necessary information for each block in a random
 * access file.
 */
public class BufferNode {
    private final long blockNum;
    private List<Record> block;
    private boolean mustWrite = false;

    /**
     * BufferNode is defined by its block number and the list containing
     * the block data
     * @param blockNum the block index in relation to the random access file
     * @param block the list of Record objects representing the block information
     */
    BufferNode(long blockNum, List<Record> block){
        this.blockNum = blockNum;
        this.block = block;
    }

    /**
     * Getter for the block number
     * @return the block number as a long
     */
    public long getBlockNum(){
        return blockNum;
    }

    /**
     * Getter for the block
     * @return the list of Record objects representing the block
     */
    public List<Record> getBlock(){
        return block;
    }

    /**
     * Setter for the block. Necessary for updating block information
     * @param b the new block to be set
     */
    public void setBlock(List<Record> b){
        block = b;
    }

    /**
     * Setter for the boolean mustWrite variable
     * @param b true if changes were made, false otherwise
     */
    public void setMustWrite(boolean b){
        mustWrite = b;
    }

    /**
     * Getter for the boolean mustWrite variable. Determines
     * if a block must be written to the file when it gets flushed
     * @return true if the block must be written, false otherwise
     */
    public boolean getMustWrite(){
        return mustWrite;
    }
}
