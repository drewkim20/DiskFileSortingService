package externalsort;

import java.io.IOException;

/**
 * The DiskFileService interface is essentially a mask for the
 * Heapsort algorithm to hide the fact it is interacting with
 * a buffer pool system instead of the actual disk file.
 */
public interface DiskFileService {
    /**
     * The service's version of reading from a disk file
     * @param offset the offset to be read
     * @return a Record from the disk file containing
     * a key and a value
     * @throws IOException for RandomAccessFile operations
     */
    Record read(long offset) throws IOException;

    /**
     * The service's version of writing to a disk file
     * @param buffNode the BufferNode representing a block of data
     *                 to be written to the disk file
     * @throws IOException for RandomAccessFile operations
     */
    void write(BufferNode buffNode) throws IOException;

    /**
     * The service's version of swapping two pieces of data (Records)
     * in a disk file. Needed for the Heapsort algorithm
     * @param r1 the first record to be swapped
     * @param r2 the second record to be swapped
     * @throws IOException for RandomAccessFile operations
     */
    void swap(Record r1, Record r2) throws IOException;

    /**
     *
     * @return the length of the RandomAccessFile as a long
     * @throws IOException for RandomAccessFile operations
     */
    long length() throws IOException;

    /**
     * Flushes as necessary to complete updating the disk file
     * after Heapsort is finished
     * @throws IOException for RandomAccessFile operations
     */
    void flush() throws IOException;
}
