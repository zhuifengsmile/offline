package udp;

import java.util.Map;

/**
 * Created by zhuifeng on 2017/6/3.
 */
public class DefaultUdp implements UDPInterface {
    @Override
    public void init() {

    }

    @Override
    public boolean process(Map<String, String> data) {
        return true;
    }

    @Override
    public void cleanup() {

    }
}
