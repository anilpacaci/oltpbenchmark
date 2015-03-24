In this wiki page we briefly describe the nature of each of the benchmarks we included in our suite.


# Wikipedia #
For wikipedia all data, schema, and queries are available since the project is open-source. We extracted the most popular operations and computed from all available information (including various interactions with the DBAs of the Wikimedia foundation) the relative frequencies of use.
The workload is based on an actual trace of user requests from Wikipedia from:


_G. Urdaneta, G. Pierre, and M. van Steen, “Wikipedia workload analysis for decentralized hosting,” Elsevier Computer Networks, vol. 53, no. 11, pp. 1830–1845, Jul. 2009. [Online](Online.md). Available: http://www.globule.org/publi/WWADH_comnet2009.html_

From this we extracted the most common operation: fetch the current version of an article. This request involves three queries: select a page id (pid) by title, select the page and revision tuples by joining the page and revision on revision.page = page.pid and revision.rid = page.latest, then finally select the text matching the text id from the revision tuple. This operation is implemented as three separate queries even though it could be one because of changes in the software over time, MySQL- specific performance issues, and the various layers of caching outside the database that we do not model.

We provide real data and real traces for wikipedia, since the entire Wikipedia and full trace are most likely to be "too much" for most intent and purposes we provide different samples of various sizes, extracted from a snapshot of English Wikipedia from January 2008 (the time corresponding to the trace we have).


# Twitter #
The Twitter benchmark is inspired by the popular micro-blogging website. In order to provide a realistic benchmark we obtained a complete and anonymized snapshot of the Twitter social graph as of August 2009 containing 51 million users and almost 2 billion follow relationships obtain by:

_M. Cha, H. Haddadi, F. Benevenuto, and K. P. Gummadi, “Measuring user influence in Twitter: The million follower fallacy,” in International Conference on Weblogs and Social Media (ICWSM), Washington DC, USA, May 2010_

We replicate the follow relationship to support lookups in both directions by an indexed key in order to access more efficiently who is being followed as well as users who are following a given user. This is the way Twitter organizes their data, as of 2010.
To simulate the application, we synthetically generated tweets and a query workload based on properties of the actual web site. Our read/write workload consists of the following operations: 1) insert a new tweet, 2) read a tweet by tweet id, 3) read the 20 most recent tweets of a certain user, 4) get the 100 most recent tweets of the people a user follows, and 5) get the names of the people that follow a user. Operations 4 and 5 are implemented via two separate queries. The result is a reasonable approximation of a few core features of Twitter.

# Epinions #
This benchmark is derived from the epinions.com product review website, we reconstructed some of the core operations performed on this website, and used actual data collected by:

_P. Massa and P. Avesani. Controversial users demand local trust metrics: an experimental study on epinions.com community. In AAAI’05, 2005._

To provide a faithful interaction between users, and items.
The Epinions.com schema contains four relations: users, items, reviews and trust. The reviews relation represents an n-to-n relationship between users and items (capturing user reviews and ratings of items). The trust relation represents a n-to-n relationship between pairs of users indicating a unidirectional “trust” value. The data was obtained by Paolo Massa from the Epinions.com development team. Since no workload is provided, we created requests that approximate the functionality of the website:
  * Q1. For a given user and item, retrieve ratings from trusted users Q2. Retrieve the list of users trusted by a given user
  * Q3. Given an item, retrieve the weighted average of all ratings Q4. Given an item, obtain the 10 most popular reviews
  * Q5. List the 10 most popular reviews of a user Q6. Insert/update the user profile
  * Q7. Insert/update the metadata of an item
  * Q8. Insert/update a review
  * Q9. Update the trust relation between two users


# YCSB #
The Yahoo! Cloud Serving Benchmark (YCSB) is a collection of simple micro-benchmarks designed to represent data management applications that are simple, but that require high scalability:

_B. F. Cooper, A. Silberstein, E. Tam, R. Ramakrishnan, and R. Sears. Benchmarking cloud serving systems with ycsb. SoCC, 2010._

This benchmark is designed to evaluate distributed key/value storage systems, such as those created by Yahoo, Google and various open source projects. In this work, the benchmark is used to pro- vide insight in some of the capabilities of our tool, e.g., its ability fall back to cheaper partitioning strategies in case of a tie, on a standardized and simple test case.

While YCSB is open source and available at: https://github.com/brianfrankcooper/YCSB/wiki/ we provide a reimplementation that uses the rest of our driving framework, for the ease of use.



# TPC-C #
The TPC-C benchmark is designed to simulate the OLTP workload of an order processing system. The implementation we used does not strictly adhere to the requirements of the TPC-C specification: http://tpc.org/tpcc/default.asp. In particular, in order to generate high throughput with small dataset sizes, the client simulators do not use the specified “think times” and instead submits the next transaction as soon as it receives the response from the first. Thus, our results in Section 6.3 are not meant to be compared with other systems, but rather indicate the relative performance of the configurations we tested.
The TPC-C schema contains 9 tables with a total of 92 columns, 8 primary keys, and 9 foreign keys. TPC-C workload contains 5 types of transactions.

# Synthetic Resource Stresser #
While most of our benchmarks are designed to emulate existing systems or common applications, we included a purely synthetic benchmark that allows the user to impose varying degrees of pressure on each of the system resources (CPU, IO, NET, MEM, LOCKs) independently.

A common use case for a benchmark is to stress-test a system. In particular, we found of great use to be able to "stress" different portions of DBMS to study the efficiency/effectiveness of the system under different workloads. To this purpose we designed a simple set of queries/transactions that imposes load almost exclusively on a single resource. For example we have very CPU-intensive queries (using encryption functionalities), that can be used to put pressure on the cpu, disk intensive queries, that are designed to generate a large amount of IOs, and lock-prone queries.

The combination of this (and other) basic queries, and the ability of the framework to control mixture, and rate over the course of a benchmark allows us to simulate very diverse kind of workloads (by combining properly this basic operations).


# TATP #
# AuctionMark #
# SEATS #
# JAPB (Hibernate) #
This is a simple port of: http://http://www.jpab.org/
This is a comprehensive benchmark that compares the performance of different combinations of JPA providers and Database Management Systems (DBMS). It covers many JPA ORM providers (Hibernate, EclipseLink, OpenJPA and DataNucleus) and DBMS (MySQL, PostgreSQL, Derby, HSQLDB, H2, HSQLite) that are available in Java.

The benchmark is published by ObjectDB Software Ltd., but it should be useful also for anyone that is interested only in RDBMS and ORM (and not in object databases).
