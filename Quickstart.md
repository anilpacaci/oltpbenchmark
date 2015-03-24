

# Introduction #

OltpBenchmark is a modular, extensible and configurable OLTP benchmarking tool. The following is a quick guide to get you started with synthetic data and sample workload mixes.

# Getting OltpBenchmark from SVN #

First of all, obtain the source code from the svn repository
```
svn checkout http://oltpbenchmark.googlecode.com/svn/trunk/ oltpbenchmark
```

The next step is to compile OltpBenchmark; we provide an Apache Ant script to automatically build the system from the source code:
```
Djellel$ ant
Buildfile: /Users/Djellel/repositories/boltp/build.xml

build:
     [echo] benchmark: /Users/Djellel/repositories/oltpbenchmark/build.xml
    [mkdir] Created dir: /Users/Djellel/repositories/oltpbenchmark/build/META-INF
     [copy] Copying 1 file to /Users/Djellel/repositories/oltpbenchmark/build/META-INF
    [javac] Compiling 150 source files to /Users/Djellel/repositories/oltpbenchmark/build
    [javac] Note: Some input files use or override a deprecated API.
    [javac] Note: Recompile with -Xlint:deprecation for details.
    [javac] Note: Some input files use unchecked or unsafe operations.
    [javac] Note: Recompile with -Xlint:unchecked for details.
     [copy] Copying 8 files to /Users/Djellel/repositories/oltpbenchmark/build

BUILD SUCCESSFUL
Total time: 3 seconds
```

# Preparing the database #
If you start from scratch, you should create an empty database (e.g., for TPC-C you can create a db named **tpcc**) and provide login credential to the benchmark, by modifying  **the workload descriptor file**. The ./config directory provides several examples, we now use the sample\_tpcc\_config.xml. You should edit the following portion:

```
    <dbtype>mysql</dbtype>
    <driver>com.mysql.jdbc.Driver</driver>
    <DBUrl>jdbc:mysql://127.0.0.1:3306/tpcc</DBUrl>
    <username>user</username>
    <password>password</password>
```

This example is for a locally (127.0.0.1) mysql answering on port 3306. Once you tested that the machine on which you run the benchmark can communicate to the DBMS using this credentials you can proceed to the next step.

# Running a Benchmark #

A utility script (./oltpbenchmark) is provided for running the system.
To run OltpBenchmark, two information are required:
  * The target benchmark (tpcc, wikipedia .. ect)
  * The workload descriptor file (samples provided under ./config)

## Target benchmarks ##

The list of supported benchmarks and options can be obtained using the --help option:

```
Djellel$ ./oltpbenchmark --help
usage: oltpbenchmark
 -b,--bench <arg>             [required] Benchmark class. Currently
                              supported: [tpcc, tatp, wikipedia,
                              resourcestresser, twitter, epinions, ycsb,
                              jpab, seats, auctionmark]
 -c,--config <arg>            [required] Workload configuration file
    --clear <arg>             Clear all records in the database for this
                              benchmark
    --create <arg>            Initialize the database for this benchmark
    --dialects-export <arg>   Export benchmark SQL to a dialects file
    --execute <arg>           Execute the benchmark workload
 -h,--help                    Print this help
    --histograms              Print txn histograms
    --load <arg>              Load data using the benchmark's data loader
 -o,--output <arg>            Output file (default System.out)
    --runscript <arg>         Run an SQL script
 -s,--sample <arg>            Sampling window
 -v,--verbose                 Display Messages

```

About every implemented benchmark comes with a data generator that creates and loads the target database with synthetic data that mimics, to the best, real world datasets.

**Example**
The following command for example initiate a tpcc database (--create=true --load=true) and a then run a workload as described in config/sample\_tpcc\_config.xml file. The results (latency, throughput) are summarized into 5seconds buckets (-s 5) and the output is written into two file: outputfile.res (aggregated) and outputfile.raw (detailed):
```
./oltpbenchmark -b tpcc -c config/sample_tpcc_config.xml --create=true --load=true --execute=true -s 5 -o outputfile
```

Since data loading can be a lengthy process, one would first create a and populate a database which can be reused for multiple experiments:
```
./oltpbenchmark -b tpcc -c config/sample_tpcc_config.xml --create=true --load=true
```

Then running an experiment could be simply done with the following command on a fresh or used database.
```
./oltpbenchmark -b tpcc -c config/sample_tpcc_config.xml --execute=true -s 5 -o outputfile
```

## Workload descriptor ##

OltpBenchmark uses a configuration file to drive a given benchmark. The workload descriptor (or configuration file) provides the general information to access the database (driver, URL, credential .. etc), benchmark specific options and most importantly, the workload mix.

When running a multi-phase experiment with varying a workload, one should provide multiple 

&lt;work&gt;

 sections with their duration, rate, and the weight of each transaction. Note: weights have to sum up to 100%.
The transactions are listed in the benchmark specific section 

&lt;transactiontypes&gt;

. The order in which the transactions are declared is the same as their respective weights.

**Example**
The following is an example of a TPCC configuration file.
```
<?xml version="1.0"?>
<parameters>
	
    <!-- Connection details -->
    <dbtype>mysql</dbtype>
    <driver>com.mysql.jdbc.Driver</driver>
    <DBUrl>jdbc:mysql://server:3306/ycsb</DBUrl>
    <username>user</username>
    <password>password</password>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    
    <!-- Scale factor is the number of warehouses in TPCC -->
    <scalefactor>32</scalefactor>
    
    <!-- The workload -->
    <terminals>200</terminals>
    <works>
        <work>
          <time>300</time>
          <rate>10000</rate>
          <weights>45,43,4,4,4</weights>
        </work>
	</works>
	
	<!-- TPCC specific -->  
   	<transactiontypes>
    	<transactiontype>
    		<name>NewOrder</name>
    	</transactiontype>
    	<transactiontype>
    		<name>Payment</name>
    	</transactiontype>
    	<transactiontype>
    		<name>OrderStatus</name>
    	</transactiontype>
    	<transactiontype>
    		<name>Delivery</name>
    	</transactiontype>
    	<transactiontype>
    		<name>StockLevel</name>
    	</transactiontype>
   	</transactiontypes>	
</parameters>
```

## Output ##

The raw output is a listing of start time (in java microseconds) and duration (micro seconds) for each transaction type.
**Example**
```
transaction type (index in config file), start time (microseconds),latency (microseconds)
3,1323686190.045091,8677
4,1323686190.050116,6800
4,1323686190.055146,3221
3,1323686190.060193,1459
4,1323686190.065246,2476
4,1323686190.070348,1834
4,1323686190.075342,1904
```

To obtain transaction per second (TPs), you can aggregate the results into windows using the -s 1 option. The throughput and different latency measures in milliseconds are reported:
```
time (seconds),throughput (requests/s),average,min,25th,median,75th,90th,95th,99th,max
0,200.200,1.183,0.585,0.945,1.090,1.266,1.516,1.715,2.316,12.656
5,199.800,0.994,0.575,0.831,0.964,1.071,1.209,1.424,2.223,2.657
10,200.000,0.984,0.550,0.796,0.909,1.029,1.191,1.357,2.024,35.835
```

Using the -o file option one can dump the output into **.res and**.raw files.