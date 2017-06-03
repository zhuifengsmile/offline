package bulkload.rowkeygenerator;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zhuifeng on 2017/6/3.
 */
public class CommonRowKeyGenerator extends AbstractRowKeyGenerator {

    @Override
    public byte[] genRowKey(String id, int prefixNum, int mod) {
        long prefix = -1;
        try {
            prefix = Long.parseLong(id) % mod;
        } catch (NumberFormatException e) {
            prefix = (id.hashCode() & Integer.MAX_VALUE) % mod;
        }
        String format = "%0" + prefixNum + "d:%s";
        return Bytes.toBytes(String.format(format, prefix, id));
    }

}
