/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

import bulkload.rowkeygenerator.IRowKeyGenerator;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.StringUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static java.lang.String.format;

/**
 * Tool to import data from a TSV file.
 * 
 * This tool is rather simplistic - it doesn't do any quoting or escaping, but
 * is useful for many data loads.
 * 
 * @see ImportTsv#usage(String)
 */
public class ImportTsv extends Configured implements Tool{
	protected static final Log LOG = LogFactory.getLog(ImportTsv.class);
	final static String NAME = "importtsv";
	final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

	final static String SKIP_LINES_CONF_KEY = "importtsv.skip.bad.lines";
	final static String BULK_OUTPUT_CONF_KEY = "importtsv.bulk.output";
	final static String COLUMNS_CONF_KEY = "importtsv.columns";
	final static String SEPARATOR_CONF_KEY = "importtsv.separator";
	final static String ROWKEY_CONF_KEY = "importtsv.hbase.rowkey";
	final static String ROWKEY_GENERATOR_CONF_KEY = "importtsv.hbase.rowkey.generator";
	final static String ROWKEY_GENERATOR_PARAM_CONF_KEY = "importtsv.hbase.rowkey.generator.param";
	//true: call prefixkey to hbase row key. 
	//false/null:keep original row key 
	final static String ROW_KEY_PREFIX = "importtsv.rowkey.prefix"; 
	final static String DEFAULT_SEPARATOR = "\t";
	final static String TIMESTAMP = "importtsv.timestamp";

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			usage("Wrong number of arguments: " + args.length);
			return -1;
		}
		setConf(HBaseConfiguration.create(getConf()));
		Configuration conf = getConf();
		// Make sure columns are specified
		String columns[] = conf.getStrings(COLUMNS_CONF_KEY);
		if (columns == null) {
			usage("No columns specified. Please specify with -D"
					+ COLUMNS_CONF_KEY + "=...");
			return -1;
		}
		// Make sure rowkey is specified
		String rowkey = conf.get(ROWKEY_CONF_KEY);
		if (StringUtil.isEmpty(rowkey)) {
			usage("No rowkey specified or rowkey is empty. Please specify with -D"
					+ ROWKEY_CONF_KEY + "=...");
			return -1;
		}
		// Make sure rowkey handler is specified
		String rowKeyGenerator = conf.get(ROWKEY_GENERATOR_CONF_KEY);
		if (StringUtil.isEmpty(rowKeyGenerator)) {
			usage("No rowkey_handler specified or rowkey generator is empty. Please specify with -D"
					+ ROWKEY_GENERATOR_CONF_KEY + "=...");
			return -1;
		}
		// Make sure they specify exactly one column as the row key
		int rowkeysFound = 0;
		for (String col : columns) {
			String[] parts = col.split(":", 3);
			if (parts.length > 1 &&  rowkey.equals(parts[1])) {
				rowkeysFound++;
			}
		}
		if (rowkeysFound != 1) {
			usage("Must specify exactly one column as "
					+ rowkey);
			return -1;
		}
		// Make sure at least one columns are specified
		if (columns.length < 1) {
			usage("One or more columns in addition to the row key are required");
			System.exit(-1);
		}

		Job job = createSubmittableJob(conf, args);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	// final static String DEFAULT_SEPARATOR = "\u0001";

	static class TsvParser {
		/**
		 * Column families and qualifiers mapped to the TSV columns
		 */
		private final byte[][] families;
		private final byte[][] qualifiers;
		private final byte[][] postfix;

		private final byte separatorByte;

		private int rowKeyColumnIndex = -1 ;

		public static String ROWKEY_COLUMN_SPEC = "";

		/**
		 * @param columnsSpecification the list of columns to parser out, comma separated.
		 * The row key should be the special token TsvParser.ROWKEY_COLUMN_SPEC
		 * @param separatorStr
		 * @param rowkeyStr
		 */
		public TsvParser(String columnsSpecification, String separatorStr, String rowkeyStr) {
			// Configure separator
			if (separatorStr.equals("\\001")){
				separatorStr = "\001";
			}
			if (separatorStr.equals("\\002")){
				separatorStr = "\002";
			}
			if (separatorStr.equals("\\t")){
				separatorStr = "\t";
			}
			if (separatorStr.equals("\\n")){
				separatorStr = "\n";
			}			
			byte[] separator = Bytes.toBytes(separatorStr);
			Preconditions.checkArgument(separator.length == 1,
					"TsvParser only supports single-byte separators hello "
							+ separator.length + "separtors::: " + separatorStr);
			separatorByte = separator[0];
			
			ROWKEY_COLUMN_SPEC = rowkeyStr;//rowkey prefix_srcfield

			// Configure columns
			ArrayList<String> columnStrings = Lists.newArrayList(Splitter
					.on(',').trimResults().split(columnsSpecification));
			
			families = new byte[columnStrings.size()][];
			qualifiers = new byte[columnStrings.size()][];
			postfix = new byte[columnStrings.size()][];

			for (int i = 0; i < columnStrings.size(); i++) {
				String str = columnStrings.get(i);
				String[] parts = str.split(":", 3);
				if (parts.length > 1 &&  ROWKEY_COLUMN_SPEC.equals(parts[1])) {
					rowKeyColumnIndex = i;
				}				
				switch (parts.length) {
				case 1:
					families[i] = str.getBytes();
					qualifiers[i] = HConstants.EMPTY_BYTE_ARRAY;
					postfix[i] = HConstants.EMPTY_BYTE_ARRAY;
					break;
				case 2:
					families[i] = parts[0].getBytes();
					qualifiers[i] = parts[1].getBytes();
					postfix[i] = HConstants.EMPTY_BYTE_ARRAY;
					break;
				case 3:
					families[i] = parts[0].getBytes();
					qualifiers[i] = parts[1].getBytes();
					postfix[i] = parts[2].getBytes();
					break;
				}
			}
		}

		public int getRowKeyColumnIndex() {
			return rowKeyColumnIndex;
		}

		public byte[] getFamily(int idx) {
			return families[idx];
		}

		public byte[] getQualifier(int idx) {
			return qualifiers[idx];
		}

		public byte[] getPostFix(int idx) {
			return postfix[idx];
		}

		public static class BadTsvLineException extends Exception {
			public BadTsvLineException(String err) {
				super(err);
			}

			private static final long serialVersionUID = 1L;
		}

		public ParsedLine parse(byte[] lineBytes, int length)
				throws BadTsvLineException {
			// Enumerate separator offsets
			ArrayList<Integer> tabOffsets = new ArrayList<Integer>(
					families.length);
			for (int i = 0; i < length; i++) {
				if (lineBytes[i] == separatorByte) {
					tabOffsets.add(i);
				}
			}
			if (tabOffsets.isEmpty()) {
				throw new BadTsvLineException("No delimiter");
			}

			tabOffsets.add(length);
			if (tabOffsets.size() > families.length) {
				throw new BadTsvLineException("Excessive columns");
			} else if (tabOffsets.size() <= getRowKeyColumnIndex()) {
				throw new BadTsvLineException("No row key");
			}
			return new ParsedLine(tabOffsets, lineBytes);
		}

		class ParsedLine {
			private final ArrayList<Integer> tabOffsets;
			private byte[] lineBytes;
			private HashMap<String, Integer> qualifierIdx = new HashMap<String, Integer>();

			ParsedLine(ArrayList<Integer> tabOffsets, byte[] lineBytes) {
				this.tabOffsets = tabOffsets;
				this.lineBytes = lineBytes;
				for (int j = 0; j < qualifiers.length; j++) {
					qualifierIdx.put(new String(qualifiers[j]), j);
				}

			}

			public int getRowKeyOffset() {
				return getColumnOffset(rowKeyColumnIndex);
			}

			public int getRowKeyLength() {
				return getColumnLength(rowKeyColumnIndex);
			}

			public int getColumnOffsetByQualifier(byte[] qualifier) {
				Integer result = qualifierIdx.get(new String(qualifier));
				if (result != null)
					return getColumnOffset(result);
				else
					return -1;
			}

			public int getColumnLengthByQualifier(byte[] qualifier) {
				Integer result = qualifierIdx.get(new String(qualifier));
				if (result != null)
					return getColumnLength(result);
				else
					return -1;
			}

			public int getColumnOffset(int idx) {
				if (idx > 0)
					return tabOffsets.get(idx - 1) + 1;
				else if (idx == 0){
					return 0;
				}else
					return -1;
			}

			public int getColumnLength(int idx) {
				if (idx >= 0) {
					return tabOffsets.get(idx) - getColumnOffset(idx);
//					if (Arrays.equals(
//							"\\N".getBytes(),
//							Arrays.copyOfRange(lineBytes,
//									getColumnOffset(idx),
//									tabOffsets.get(idx)))) {
//						//null field in hive
//						return 0;
//					}
				} else {
					return -1;
				}


			}

			public int getColumnCount() {
				return tabOffsets.size();
			}

			public byte[] getLineBytes() {
				return lineBytes;
			}
		}

	}

	/**
	 * Write table content out to files in hdfs.
	 */
	static class TsvImporter extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		private ByteArrayOutputStream baos = new ByteArrayOutputStream();
		/** Timestamp for all inserted rows */
		private long ts;

		/** Should skip bad lines */
		private boolean skipBadLines;
		private Counter badLineCount;

		private TsvParser parser;

		private IRowKeyGenerator rowkeyGenerator = null;
		private String rowkeyParam = "";

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();

			// If a custom separator has been used,
			// decode it back from Base64 encoding.
			String separator = conf.get(SEPARATOR_CONF_KEY);
			if (separator == null) {
				separator = DEFAULT_SEPARATOR;
			} else {
				separator = new String(Base64.decode(separator));
			}

			skipBadLines = context.getConfiguration().getBoolean(
					SKIP_LINES_CONF_KEY, true);
			badLineCount = context.getCounter("ImportTsv", "Bad Lines");

			parser = new TsvParser(conf.get(COLUMNS_CONF_KEY), separator, conf.get(ROWKEY_CONF_KEY));
			if (parser.getRowKeyColumnIndex() == -1) {
				throw new RuntimeException("No row key column specified");
			}

			try {
				Class rowkeyCls = Class.forName(conf.get(ROWKEY_GENERATOR_CONF_KEY));
				rowkeyGenerator = (IRowKeyGenerator) rowkeyCls.newInstance();

			} catch (Exception e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
			rowkeyParam = conf.get(ROWKEY_GENERATOR_PARAM_CONF_KEY);
			String timestamp = conf.get(TIMESTAMP);
			if (StringUtil.isEmpty(timestamp)) {
				ts = System.currentTimeMillis();
			} else {
				ts = Long.parseLong(timestamp);
			}

		}

		/**
		 * Convert a line of TSV text into an HBase table row.
		 */
		@Override
		public void map(LongWritable offset, Text value, Context context)
				throws IOException {

			try {
				byte[] lineBytes = value.getBytes();
				TsvParser.ParsedLine parsed = parser.parse(lineBytes,
						value.getLength());
				ImmutableBytesWritable rowKey = new ImmutableBytesWritable(
						lineBytes, parsed.getRowKeyOffset(),
						parsed.getRowKeyLength());

                if (rowKey.getLength() == 0) {
                    context.getCounter("ImportTsv", "0 length rowkey").increment(1);
                    return;
                }

				String keyStr = new String(rowKey.copyBytes());
				byte[] actualRowkey = rowkeyGenerator.genRowKey(keyStr, rowkeyParam);
				if (actualRowkey.length == 0) {
					context.getCounter("ImportTsv", "0 length rowkey").increment(1);
					return;
				}
				Put put = new Put(actualRowkey);
				byte[] qualifier = null;
				for (int i = 0; i < parsed.getColumnCount(); i++) {
					if ( parsed.getColumnLength(i) == -1 ) {
						continue;
                    }
                    //nested table field
					if (parser.getPostFix(i) != HConstants.EMPTY_BYTE_ARRAY) {
						baos.reset();
						baos.write(parser.getQualifier(i));
						baos.write(':');
						baos.write(lineBytes, parsed
								.getColumnOffsetByQualifier(parser
										.getPostFix(i)), parsed
								.getColumnLengthByQualifier(parser
										.getPostFix(i)));
						qualifier = baos.toByteArray();
					} else {// normal table field
						qualifier = parser.getQualifier(i);
					}
					KeyValue kv = new KeyValue(actualRowkey,
							parser.getFamily(i), 
							qualifier,
							ts,
							KeyValue.Type.Put, 
							Arrays.copyOfRange(lineBytes,parsed.getColumnOffset(i),parsed.getColumnOffset(i)+parsed.getColumnLength(i))
							);

					put.add(kv);
				}
				context.write(new ImmutableBytesWritable(actualRowkey), put);
			} catch (TsvParser.BadTsvLineException badLine) {
				if (skipBadLines) {
					System.err.println("Bad line at offset: " + offset.get()
							+ ":\n" + badLine.getMessage());
					badLineCount.increment(1);
					return;
				} else {
					throw new IOException(badLine);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Sets up the actual job.
	 * 
	 * @param conf
	 *            The current configuration.
	 * @param args
	 *            The command line parameters.
	 * @return The newly created job.
	 * @throws IOException
	 *             When setting up the job fails.
	 */
	public static Job createSubmittableJob(Configuration conf, String[] args)
			throws IOException {

		Job job = null;
		try (Connection connection = ConnectionFactory.createConnection(conf)) {
			try (Admin admin = connection.getAdmin()) {
				// Support non-XML supported characters
				// by re-encoding the passed separator as a Base64 string.
				String actualSeparator = conf.get(SEPARATOR_CONF_KEY);
				if (actualSeparator != null) {
					conf.set(SEPARATOR_CONF_KEY,
							Base64.encodeBytes(actualSeparator.getBytes()));
				}
				TableName tableName = TableName.valueOf(args[0]);
				if (!admin.tableExists(tableName)) {
					String errorMsg = format("Table '%s' does not exist.", tableName);
					LOG.error(errorMsg);
					throw new TableNotFoundException(errorMsg);
				}
				Path inputDir = new Path(args[1]);
				String jobName = conf.get(JOB_NAME_CONF_KEY,NAME + "_" + tableName.getNameAsString());
				job = Job.getInstance(conf, jobName);
				job.setJarByClass(TsvImporter.class);
				FileInputFormat.setInputPaths(job, inputDir);
				job.setInputFormatClass(TextInputFormat.class);
				job.setMapperClass(TsvImporter.class);

				String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
				if (hfileOutPath != null) {
					try (HTable table = (HTable)connection.getTable(tableName)) {
						Path outputDir = new Path(hfileOutPath);
						FileOutputFormat.setOutputPath(job, outputDir);
						job.setMapOutputKeyClass(ImmutableBytesWritable.class);
						job.setMapOutputValueClass(Put.class);
						job.setReducerClass(PutSortReducer.class);
						HFileOutputFormat2.configureIncrementalLoad(job, table, table);
					}
				} else {
					// No reducers. Just write straight to table. Call
					// initTableReducerJob
					// to set up the TableOutputFormat.
					TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), null,
							job);
					job.setNumReduceTasks(0);

//					TableMapReduceUtil.addDependencyJars(job);
//					TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
//							com.google.common.base.Function.class /* Guava used by TsvParser */);
				}

				// Workaround to remove unnecessary hadoop dependencies
				String [] jars = job.getConfiguration().get("tmpjars").split(",", -1);
				StringBuilder filteredJars = new StringBuilder();
				for (String j: jars) {
					String [] parts = j.split("/", -1);
					String fileName = parts[parts.length - 1];
					if (fileName.indexOf("hadoop-") != 0) {
						filteredJars.append(j);
						filteredJars.append(",");
					}
				}
				job.getConfiguration().set("tmpjars", filteredJars.toString());
			}
		}


		return job;
	}

	/*
	 * @param errorMsg Error message. Can be null.
	 */
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			System.err.println("ERROR: " + errorMsg);
		}
		String usage = "Usage: "
				+ NAME
				+ " -Dimporttsv.columns=a,b,c <tablename> <inputdir>\n"
				+ "\n"
				+ "Imports the given input directory of TSV data into the specified table.\n"
				+ "\n"
				+ "The column names of the TSV data must be specified using the -Dimporttsv.columns\n"
				+ "option. This option takes the form of comma-separated column names, where each\n"
				+ "column name is either a simple column family, or a columnfamily:qualifier. The special\n"
				+ "column name HBASE_ROW_KEY is used to designate that this column should be used\n"
				+ "as the row key for each imported record. You must specify exactly one column\n"
				+ "to be the row key.\n"
				+ "\n"
				+ "In order to prepare data for a bulk data load, pass the option:\n"
				+ "  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output\n" + "\n"
				+ "Other options that may be specified with -D include:\n"
				+ "  -D" + SKIP_LINES_CONF_KEY
				+ "=false - fail if encountering an invalid line\n" + "  '-D"
				+ SEPARATOR_CONF_KEY
				+ "=|' - eg separate on pipes instead of tabs";
		System.err.println(usage);
	}

	/**
	 * Main entry point.
	 * 
	 * @param args
	 *            The command line parameters.
	 * @throws Exception
	 *             When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new ImportTsv(), args);
		System.exit(status);

	}

}
