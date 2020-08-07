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

        dingdingSinkTableInfo.setKeyWord(String.valueOf(props.get(DingdingSinkTableInfo.KEYWORD)));

        String msgType = (String) props.getOrDefault(DingdingSinkTableInfo.TEXTTYPE, "markdown");

        dingdingSinkTableInfo.setTextType(msgType);

        dingdingSinkTableInfo.setSecretKey((String) props.getOrDefault(DingdingSinkTableInfo.SECRETKEY, ""));


        String mapStr = (String) props.getOrDefault(DingdingSinkTableInfo.NAMEMAP, "");
        if (mapStr.length() > 0) {
            Gson gson = new Gson();
            Map<String, String> map = new HashMap<String, String>();
            map = gson.fromJson(mapStr, map.getClass());
            dingdingSinkTableInfo.setNameMap(map);
        } else {
            Map<String, String> map = new HashMap<>();
            map.put("w_level", "告警级别");
            map.put("w_host", "主机");
            map.put("w_ip", "主机地址");
            map.put("w_time", "时间");
            map.put("w_status", "当前状态");
            map.put("w_info", "告警信息");
            map.put("w_detail", "问题详情");
            map.put("w_id", "事件ID");
            map.put("w_range", "问题影响范围");
            map.put("w_linkuser", "问题联系人");
            dingdingSinkTableInfo.setNameMap(map);
        }


        dingdingSinkTableInfo.check();

        return dingdingSinkTableInfo;
    }
}
