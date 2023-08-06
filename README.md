# Project 3 — External Heapsort

## Learning objectives 

This project directly addresses the following course learning objectives:

* Use a standard debugger
* Develop and properly organise multi-source projects
* Use modularity in building a project
* Use module-level testing (This will be particularly important for your success on this project)
* Implement and evaluate for use a selection of object-oriented design patterns
* Individually write and test mid-sized object-oriented software modules of a professional quality, using a standard OO programming language, and using the concepts listed above

## Background

You will implement an external sorting algorithm for binary data.
This project is a little different from previous projects in that you will be doing disk I/O on a Random Access File.
The input datafile will consist of many 4-byte records, with each record consisting of 2-byte (`short`) integer values in the range `1` to `30,000`. The first 2-byte field is the key value (used for sorting) and the second 2-byte field contains a data value.
The input file is guaranteed to be a multiple of 4096 bytes.
All I/O operations will be done on blocks of size 4096 blocks (i.e., 1024 logical records).

**Warning: The data is in a binary file, not a text format.** This means you cannot simply use a `Scanner` to read its contents like you would a text file. 

## Primary task

**Note:** It is worth refreshing your understanding of the Heap data structure and the Heapsort algorithm before reading the rest of this spec. See resources shared in the schedule.

Your job is to sort the file in ascending (smallest-to-largest) order, using a modified version of the Heapsort algorithm.
The modification comes in the interaction between the Heapsort algorithm and the file storing the data.
The heap "array" will be the file itself, rather than an array stored in memory.

All accesses to the file will be mediated by a Buffer Pool. The Buffer Pool will be organised using the Least Recently Used (LRU) replacement scheme.
See resources in Canvas to remind yourself of how this works.

## Design considerations

The primary design concern of this project is the interaction between the Heap as viewed by the Heapsort algorithm, and the logical representation of the Heap as implemented by the disk file mediated by the Buffer Pool.
Recognise that this is an example of the **[Proxy design pattern](https://refactoring.guru/design-patterns/proxy)**. The Buffer Pool is the "proxy object" that controls access to the Random Access File. So any time the Heap is reading from or writing to its "backing array" (the Random Access File), it's actually going through the Buffer Pool.

**Good design will go a long way toward simplifying this project**.
There are many moving parts here. For example,

* The Heap
* The fact that you're dealing with binary data (`byte[]` instead of `String`) in a Random Access File instead of an array in memory 
* The fact that each item in the Heap is not simply, say, an integer, but rather a _record_ in which the first field is the key, and the second field is the value. You will sort based on only the key, but both the key and value have to move around together during Heapsort.
* The Buffer Pool mediating access to the Random Access File. 

You can simplify your development process by breaking the problem down and attacking parts individually. 
This may involve writing "fake" versions of certain components while you implement other components. For example, the Heap depends on having _some data source_, not necessarily a Random Access File controlled by a Buffer Pool. Can you abstract that away so it can be implemented at a later time, after you have implemented and tested your Heap?

## Invocation and I/O Files

In the starter code provided (see the section on "Starter Code"), there is a `Driver.java` as usual. I will test your program using a pre-prepared suite of automated tests. 
Since I don't have insight into your internal design, the `main` method in `Driver` is the only thing I can call in your program. _Do not move or rename that file or method_.

The program will be invoked as follows through the command line.
Please refresh your memory on how to accept command line arguments in Java programs.

```
Driver <data-file-name> <num-buffers>
```

The data file `<data-file-name>` is the file to be sorted.
The sorting takes place in that file, so this program **does** modify the input file. Be careful to keep a copy of the original when you do your testing.

The parameter `<num-buffers>` determines the number of buffers allocated for the buffer pool. This value will be in the range `1–20`.

At the end of your program, the data file (on disk) should be in a sorted state. Do not forget to flush buffers from your buffer pool as necessary at the end, or they will not update the file correctly.

### Required output 

In addition to sorting the data file, you must report some information about the execution of the program. Formatting instructions follow the descriptions.

**You will need to report part of the sorted data file to standard output.** Specifically, your program will `System.out.println` the first record from each 4096-byte block, in order, from the sorted datafile. The records are to be printed 8 records to a line (showing both the key value and the data value for each record), the values separated by whitespace. This program output must appear _exactly_ as described, because it will be used in testing.

For example, in your starter code, I have given you a sample input file (`sample_input.dat`) and the same file after sorting (`sorted_sample.dat`).
`sample_input.dat` contains 4 blocks of 4096 bytes, so after sorting the file, you should print the first record from each block (for a total of 4 records).

This is what you would print. Each pair of numbers is a record (recall that records are key-value pairs).

```
5 8404    8131 244    16634 2746    24619 6627
``` 

**After the information described above, on a new line print the word `"STATS"`. And then on the following lines you will output some statistics and information about your buffer pool usage.**

The information to write is as follows:

- The name of the data file being sorted
- The number of cache hits, or times your program found what it needed in the Buffer Pool and did not have to go to disk 
- The number of cache misses, or times your program did NOT find the data it needed in the Buffer Pool and had to do to disk
- The number of disk reads, or times your program had to read a block of data from disk into a buffer
- The time that your heapsort took to execute the Heaport. Put two calls to the stadard Java timing method `System.currentTimeMillis()` in your program. One when you call the sort function, and one when you return from the sort function. This method returns a `long` value. The difference between the two values will be total runtime in milliseconds. You should ONLY time the sort, and not the time taken to write statfile output.

For the same example input file above, my output written to the statfile would look like:

```
STATS
File name: sample_input.dat
Cache hits: 348783
Cache misses: 8745
Disk reads: 8745
Disk writes: 8202
Time to sort: 125
```

In summary, if I invoked heap sort on `sample_input.dat` with **2 buffers**, the printed output will look like

```
5 8404    8131 244    16634 2746    24619 6627
STATS
File name: sample_input.dat
Cache hits: 348783
Cache misses: 8745
Disk reads: 8745
Disk writes: 8202
Time to sort: 125
```

It's important that you adhere to these output rules, since automated tests will depend on them.

## Starter code

I have provided the following files for you:

**`Driver`**

This is the file containing the `main` method, the entry point for your program. It accepts command-line parameters as described in the "Invocation and I/O Files" section.

**`Utils`**

This is a utility class that contains a couple of utilities for you to use on this project.

The `checkFile` method takes in a file name and checks if the records in the file are sorted correctly. You can use this for your testing.

The `generateByteFile` takes in an integer for number of records and a name of a file, and generates a data file of randomly chosen records for testing.
Note that this `generateByteFile` method should only be used when you want to generate large files for stress-testing your program when large portions of it are already in place.
If you're testing, for example, your Heapsort, you should be following the systematic specification-based testing process we talked about earlier this quarter.

In addition, I have provided **`sample_input.dat`** and **`sorted_sample.dat`**. My tests will include more testing files, but testing against these samples should give you an initial idea of how your project is progressing. 

The files are included in your starter repository, but in case you overwrite them by accident, here are copies:

* **`sample_input.dat`** (**LINK IN CANVAS**)
* **`sorted_sample.dat`** (**LINK IN CANVAS**)


