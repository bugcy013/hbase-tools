HBase Monitoring Tools
===========

Collection of scripts to run synthetic transactions again HBase to see if it's operational.

## How do I use this?
1. Download the **hbase-transaction.rb** script to a location on your HBase server (For example: HBase Master)
2. Run the script using the hbase-jruby wrapper. This should run a synthetic transaction and exit based on success

## Sample Output
    [root@server:~]# /usr/lib/hbase/bin/hbase-jruby /tmp/hbase-transaction.rb
    SLF4J: Class path contains multiple SLF4J bindings.
    SLF4J: Found binding in [jar:file:/usr/lib/hbase/lib/slf4j-log4j12-1.6.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
    SLF4J: Found binding in [jar:file:/usr/lib/zookeeper/lib/slf4j-log4j12-1.6.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
    SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
    Cleaning old transaction tables
    Checking: table1
    Checking: table2
    Creating hbase-transaction-test_1371080155_330
    Table verified to exist
    Lets create some data
    Lets get some data
    String matched
    Dropping hbase-transaction-test_1371080155_330 and exiting 0
