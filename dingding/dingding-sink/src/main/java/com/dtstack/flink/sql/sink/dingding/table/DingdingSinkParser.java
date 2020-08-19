package com.dtstack.flink.sql.sink.dingding.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class DingdingSinkParser extends AbstractTableParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        DingdingSinkTableInfo dingdingSinkTableInfo = new DingdingSinkTableInfo();
        dingdingSinkTableInfo.setName(tableName);

        parseFieldsInfo(fieldsInfo, dingdingSinkTableInfo);

        dingdingSinkTableInfo.setToken(String.valueOf(props.get(DingdingSinkTableInfo.TOKEN)));

        if (props.get(DingdingSinkTableInfo.MOBILES) != null) {
            String[] mobiles = String.valueOf(props.get(DingdingSinkTableInfo.MOBILES)).split("\\,");

            dingdingSinkTableInfo.setMobiles(mobiles);
        }

        if (props.get(DingdingSinkTableInfo.DISTINCT) != null) {
            String[] distincts = String.valueOf(props.get(DingdingSinkTableInfo.DISTINCT)).split("\\,");
            dingdingSinkTableInfo.setDistincts(distincts);
        }

        String time = (String) props.getOrDefault(DingdingSinkTableInfo.TIMEINTERNAL, 10 * 60 + "");

        dingdingSinkTableInfo.setTimeInternal(time);

        dingdingSinkTableInfo.setKeyWord(String.valueOf(props.getOrDefault(DingdingSinkTableInfo.KEYWORD, "监控告警")));

        String msgType = (String) props.getOrDefault(DingdingSinkTableInfo.TEXTTYPE, "markdown");

        dingdingSinkTableInfo.setTextType(msgType);

        dingdingSinkTableInfo.setSecretKey((String) props.getOrDefault(DingdingSinkTableInfo.SECRETKEY, ""));


        dingdingSinkTableInfo.check();

        return dingdingSinkTableInfo;
    }
}
