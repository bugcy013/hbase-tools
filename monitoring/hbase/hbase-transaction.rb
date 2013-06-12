#!/usr/bin/env hbase-jruby
# Copyright 2011 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# Performs a synthetic transaction on an HBase system for monitoring purposes

include Java 
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
require 'timeout'

def clean_transaction_tables(admin)
  admin.listTables.each do |table|
    table_name = table.getNameAsString
    puts 'Checking: ' + table_name
    if table_name.include?('hbase-transaction-test')
      puts 'Cleaning (Dropping) table: ' + table_name
      begin
        admin.disableTable(table_name)
        admin.deleteTable(table_name)
      rescue Exception => e
        puts 'Error dropping ' + table_name + ' : ' + e
      end
    end
  end
end

def drop_and_exit(admin, table, exit_code)
  puts 'Dropping ' + table.to_s + ' and exiting ' + exit_code.to_s
  if admin.tableExists(table)
    admin.disableTable(table)
    admin.deleteTable(table)
  end
  exit exit_code
end

begin
  Timeout.timeout(30) do

    # disable debug/info logging on this script for clarity
    log_level = org.apache.log4j.Level::ERROR
    org.apache.log4j.Logger.getLogger('org.apache.hadoop.hbase').setLevel(log_level)
    org.apache.log4j.Logger.getLogger('org.apache.zookeeper').setLevel(log_level)

    # transaction table
    tablename = 'hbase-transaction-test_' + Time.now.to_i.to_s + '_' + rand(1000).to_s
    exit_code = 0
    desc = HTableDescriptor.new(tablename)
    desc.addFamily(HColumnDescriptor.new('testfamily'))

    # create configuration / admin interface
    config = HBaseConfiguration.create
    admin = HBaseAdmin.new(config)

    # if table exists, then drop it
    puts 'Cleaning old transaction tables'
    clean_transaction_tables(admin)

    puts "Creating %s" % tablename
    # create the table
    admin.createTable(desc)

    # verify the table exists
    if admin.tableExists(tablename)
      puts 'Table verified to exist'
    else
      puts 'Could not create table'
      drop_and_exit(admin, tablename, 1)  
    end

    # lets put some data
    puts 'Lets create some data'
    table = HTable.new(config, tablename)
    row = Bytes.toBytes('testrow')
    family = Bytes.toBytes('testfamily')
    column = Bytes.toBytes('testcolumn')

    # put the operation
    p = Put.new(row)
    p.add(family, column, Bytes.toBytes('hbase_check'))
    table.put(p)

    # fetch the data back
    puts 'Lets get some data'
    g = Get.new(row)
    result = table.get(g)
    data = result.getValue(family, column)
    data_s = String.from_java_bytes data
    if data_s == 'hbase_check'
      puts 'String matched'
      drop_and_exit(admin, tablename, 0)
    end
    puts 'String did not match'
    drop_and_exit(admin, tablename, 1)
  end
rescue Timeout::Error => e
  puts "#{Time.now}: JRuby Timeout: #{e}"
  exit 3
end
