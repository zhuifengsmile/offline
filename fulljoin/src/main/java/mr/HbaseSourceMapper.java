package mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * Created by zhuifeng on 2017/5/31.
 */
public class HbaseSourceMapper extends TableMapper<NullWritable, Text> {
    private final static String DOC_BEGIN = "<doc>";
    private final static String DOC_END = "</doc>";
    private final static String KV_SEPARATOR = "=";
    private final static String RECORD_SEPARATOR = "\001\t";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        NavigableMap<byte[], NavigableMap<byte[], byte[]>> result = value.getNoVersionMap();
        if(result.isEmpty()){
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(DOC_BEGIN);
        for(NavigableMap.Entry<byte[], NavigableMap<byte[], byte[]>> familyEntry : result.entrySet()){
            for(NavigableMap.Entry<byte[], byte[]> entry : familyEntry.getValue().entrySet()){
                sb.append(Bytes.toString(entry.getKey()))
                        .append(KV_SEPARATOR)
                        .append(Bytes.toString(entry.getValue()))
                        .append(RECORD_SEPARATOR);
            }
        }
        sb.append(DOC_END);
        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}
