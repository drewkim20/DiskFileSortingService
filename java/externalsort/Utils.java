package externalsort;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Utility functions to help with testing this Heapsort package.
 */
public class Utils {

    private static final Random RANDOM = new Random();


    // Private constructor to prevent instantiation.
    private Utils() {}

    /**
     * This method checks a file to see if it is properly sorted.
     *
     * @param filename
     *            a string containing the name of the file to check
     * @return true if the file is sorted, false otherwise
     * @throws IOException if the file is not found or there are other issues
     *  with reading the file.
     */
    // Suppress warning from un-queries recordCount variable, and
    // commented-out print statement in catch clause
    @SuppressWarnings({"java:S1481", "java:S125"})
    public static boolean checkFile(String filename) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)))) {
            // Prime with the first record
            short key2 = in.readShort();
            in.readShort();
            int recordCount = 0;
            try {
                while (true) {
                    short key1 = key2;
                    recordCount++;
                    key2 = in.readShort();
                    in.readShort();
                    if (key1 > key2){
                        return false;
                    }
                }
            } catch (EOFException e) {
                // Reached the end of the file, no action needed.
                // Examine recordCount if you want to examine the number of records that were processed.
                // System.out.println(recordCount + " records processed");
            }
        }

        return true;
    }

    /**
     * Generates a binary file of random data for testing.
     *
     * @param numRecords The number of records that should be in the file.
     *                   This must be a multiple of 1024. Each record is 4 bytes long.
     * @param fileName The name of the file to which data should be written.
     *                 This method will OVERWRITE the file if a file with the same
     *                 name already exists.
     * @throws IllegalArgumentException if numRecords is not a multiple of 1024
     * @throws IOException if the file is not writable or there are other issues with the file
     */
    public static void generateByteFile(int numRecords, String fileName) throws IOException {
        if (numRecords % 1024 != 0) {
            throw new IllegalArgumentException("numRecords must be a multiple of 1024");
        }
        File file = new File(fileName);
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file, false))) {
            for (int i = 0; i < numRecords; i++) {
                short key = (short) RANDOM.nextInt(Short.MAX_VALUE);
                short value = (short) RANDOM.nextInt(Short.MAX_VALUE);

                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putShort(key);
                bb.putShort(value);
                out.write(bb.array());
            }
        }
    }
}
