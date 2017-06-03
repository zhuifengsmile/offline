package udp;

import java.util.Map;

/**
 * Created by zhuifeng on 2017/6/3.
 */
public interface UDPInterface {

    public void init();
    
    /**
     * @param data
     * @return true if this record is valid,otherwise return false
     * if throws execption,we will discard this record
     */
    public boolean process(Map<String, String> data);

    public void cleanup();
}
