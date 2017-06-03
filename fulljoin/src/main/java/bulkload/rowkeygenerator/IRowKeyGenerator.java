package bulkload.rowkeygenerator;

/**
 * Created by zhuifeng on 2017/6/3.
 */
public interface IRowKeyGenerator {

    public byte[] genRowKey(String id, String param);
}
