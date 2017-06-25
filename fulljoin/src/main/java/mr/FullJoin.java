package mr;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import outputformat.TextValueOutputFormat;
import utils.StringUtil;

import javax.annotation.Nullable;
import java.io.IOException;

import static utils.Constants.*;

/**
 * Created by zhuifeng on 2017/5/31.
 */
public class FullJoin extends Configured implements Tool {
    private final static String SCAN_FAIMILIES = "families";
    private final static String OUTPUT_COMPRESSS = "output.compress";
    private Scan scan;
    private String primaryTable;
    private Path outputPath;
    @Override
    public int run(String[] args) throws Exception {
        init(args);
        Job job = Job.getInstance(getConf(), "scan_hbase_to_hdfs");
        job.setInputFormatClass(TableInputFormat.class);
        TableMapReduceUtil.initTableMapperJob(
                primaryTable,
                scan,
                HbaseSourceMapper.class,
                NullWritable.class,
                Text.class,
                job
        );
        job.setOutputFormatClass(TextValueOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        TextValueOutputFormat.setOutputPath(job, outputPath);
        if(getConf().getBoolean(OUTPUT_COMPRESSS, false)){
            TextValueOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        }else {
            TextValueOutputFormat.setCompressOutput(job, false);
        }
        return job.waitForCompletion(true) ? 0 : -1;
    }
    private void init(String[] args) throws ParseException, IOException {
        this.scan = new Scan();
        Options option = new Options();
        option.addOption("f", true, "xml config");
        option.addOption("udpClassName", false, "udpClassName");
        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(option, args);

        Configuration conf = getConf();
        if(cl.hasOption("f")){
            conf.addResource(cl.getOptionValue("f"));
        }
        String families = conf.get(SCAN_FAIMILIES);
        if(null != families){
            String[] familyArray = families.split(",");
            for(String family : familyArray){
                scan.addFamily(Bytes.toBytes(family));
            }
        }

        scan.setCaching(100);
        scan.setCacheBlocks(false);

        primaryTable = conf.get("primaryTable");
        checkArgument(StringUtil.isNotEmpty(primaryTable), "primaryTable is empty for not set");
        String outputDir = conf.get("hdfs.output.path");
        checkArgument(StringUtil.isNotEmpty(outputDir), "hdfs.output.path is empty for not set");
        outputPath = new Path(outputDir);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            if(!fs.delete(outputPath, true)){
                throw new IllegalStateException("delete path:" + outputPath + " failed");
            }
        }
        getConf().set(UDP_CLASS_NAME, cl.getOptionValue("udpClassName", ""));
    }
    private static void checkArgument(boolean condiction, @Nullable Object errorMsg){
        if(!condiction){
            throw new IllegalArgumentException(errorMsg.toString());
        }
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(HBaseConfiguration.create(), new FullJoin(), args);
        System.exit(res);
    }
}
