package mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import udp.DefaultUdp;
import udp.UDPInterface;
import utils.NestedTableAggregator;
import utils.StringUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static utils.Constants.*;

/**
 * Created by zhuifeng on 2017/5/31.
 */
public class HbaseSourceMapper extends TableMapper<NullWritable, Text> {
    private final static String DOC_BEGIN = "<doc>\001\n";
    private final static String DOC_END = "</doc>\001\n";
    private final static String KV_SEPARATOR = "=";
    private final static String RECORD_SEPARATOR = "\001\n";
    private UDPInterface udpProcessor;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String udpClassName = context.getConfiguration().get(UDP_CLASS_NAME);
        if(StringUtil.isEmpty(udpClassName)){
            udpProcessor = new DefaultUdp();
        }else{
            try {
                udpProcessor = (UDPInterface) Class.forName(udpClassName).newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        NavigableMap<byte[], NavigableMap<byte[], byte[]>> result = value.getNoVersionMap();
        if(result.isEmpty()){
            return;
        }
        Map<String, String> fieldMap = new HashMap<>();
        for(NavigableMap.Entry<byte[], NavigableMap<byte[], byte[]>> familyEntry : result.entrySet()){
            for(NavigableMap.Entry<byte[], byte[]> entry : familyEntry.getValue().entrySet()){
                fieldMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
        }
        NestedTableAggregator.aggregate(fieldMap);

        if(!udpProcessor.process(fieldMap)){
            context.getCounter("HbaseSourceMapper", "filter records").increment(1);
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(DOC_BEGIN);
        for(Map.Entry<String, String> entry : fieldMap.entrySet()){
            sb.append(entry.getKey())
                    .append(KV_SEPARATOR)
                    .append(entry.getValue())
                    .append(RECORD_SEPARATOR);
        }
        sb.append(DOC_END);
        context.getCounter("HbaseSourceMapper", "output records").increment(1);
        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}
