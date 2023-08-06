package externalsort;

/**
 * The Record class holds information of each individual
 * key-value pair in the disk file
 */
public class Record {
    private long offset;
    private final short key;
    private final short value;

    /**
     *
     * @param offset the offset in the disk file as a long
     * @param key the key of the key-value pair as a short
     * @param value the value of the key-value pair as a short
     */
    public Record(long offset, short key, short value){
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    /**
     * Getter for the offset of the record
     * @return the offset as a long
     */
    public long getOffset(){
        return offset;
    }

    /**
     * Getter for the key of the record
     * @return the key as a short
     */
    public short getKey(){
        return key;
    }

    /**
     * Getter for the value of the record
     * @return the value as a short
     */
    public short getValue(){
        return value;
    }

    /**
     * Setter for the offset of the record
     * @param o the updated offset
     */
    public void setOffset(long o){
        offset = o;
    }

    /**
     * Overriding the equals method so that two Records are equal
     * if they have equal offsets, keys, and values
     * @param o the object to be compared with this Record
     * @return true if equal, false otherwise
     */
    @Override
    public boolean equals(Object o){
        return o instanceof Record r && r.offset == this.offset &&
                r.key == this.key && r.value == this.value;
    }

    /**
     * Conventionally overriding the hashCode() method since we
     * overrode the equals() method.
     * @return a newly defined hash code
     */
    @Override
    public int hashCode(){
        return (int)(2 * offset + 3 * key + 4 * value);
    }
}
