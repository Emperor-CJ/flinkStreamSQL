package com.dtstack.flink.sql.sink.dingding;

public class Level {
    public static String levelTitleInfo(int level) {
        switch (level) {
            case 0:
                return "告警恢复";
            case 1:
                return "告警等级: 一般严重";
            default:
                return "监控告警";
        }
    }

    public static String levelInfo(int level) {
        switch (level) {
            case 0:
                return "恢复";
            case 1:
                return "告警";
            default:
                return "告警";
        }
    }
}
