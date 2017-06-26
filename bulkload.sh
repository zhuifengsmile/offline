#!/bin/bash
/home/admin/hadoop/bin/hadoop --config /home/admin/hadoop/etc/hadoop  jar ./fulljoin/target/fulljoin-1.0-SNAPSHOT.jar bulkload.ImportTsv -D mapreduce.job.queuename=defualt   -D mapreduce.job.name=importtsv_to_hbase   -D importtsv.separator='\001'    -D hbase.mapreduce.hfileoutputformat.blocksize=65535   -D importtsv.columns=cf:itemId,cf:price,cf:weight   -D importtsv.hbase.rowkey=itemId   -D importtsv.hbase.rowkey.generator=bulkload.rowkeygenerator.CommonRowKeyGenerator   -D importtsv.hbase.rowkey.generator.param=1000:4   -D importtsv.bulk.output=/test/dump/temp_hfile/hfile_source_hdfs_dir   -D importtsv.timestamp=1471838841894    test_hbase_table_name  /test/dump/hfile_source_hdfs_dir/

if [ $? == 0 ] ; then
    echo "ImportTsv SUCCEED"
else
    echo "ImportTsv FAILED"
    exit 1
fi
/home/admin/hadoop/bin/hadoop --config /home/admin/hadoop/etc/hadoop   org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles  /test/dump/temp_hfile/hfile_source_hdfs_dir test_hbase_table_name

if [ $? == 0 ] ; then
    echo "LoadIncrementalHFiles SUCCEED"
    exit 0
else
    echo "LoadIncrementalHFiles FAILED"
    exit 1
fi
