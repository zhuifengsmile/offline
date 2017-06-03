package bulkload.rowkeygenerator;

import com.google.common.base.Preconditions;
import utils.StringUtil;

/**
 * Created by zhuifeng on 2017/6/3.
 */
public abstract class AbstractRowKeyGenerator implements IRowKeyGenerator{


    protected static int DEFAULT_PREFIX_NUM = 4;
    protected static int DEFAULT_MOD =  1000;

    public byte[] genRowKey(String id) {
        return genRowKey(id,DEFAULT_PREFIX_NUM, DEFAULT_MOD);
    }

    public byte[] genRowKey(String id, int prefixNum) {
        return genRowKey(id,prefixNum, DEFAULT_MOD);
    }

    abstract public byte[] genRowKey(String id, int prefixNum, int mod);

    @Override
    public byte[] genRowKey(String id, String param) {
        Preconditions.checkArgument(StringUtil.isEmpty(param),
                "rowKey handler params error:" + param + ",should like 4:1000");
        String[] params = param.split(":");
        Preconditions.checkArgument( 2 != params.length,
                "rowKey handler params error:" + param + ",should like 4:1000");
        int prefixNum = Integer.parseInt(params[0]);
        int mod = Integer.parseInt(params[1]);
        return genRowKey(id, prefixNum, mod);
    }
}
