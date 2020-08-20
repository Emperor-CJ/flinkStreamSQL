package com.dtstack.flink.sql.sink.dingding.table;

import com.dtstack.flink.sql.table.AbstractTargetTableInfo;

import java.util.Arrays;

public class DingdingSinkTableInfo extends AbstractTargetTableInfo {

    private static final String CURR_TYPE = "dingding";

    public static final String TOKEN = "token";

    public static final String MOBILES = "mobiles";

    public static final String DISTINCT = "distincts";

    public static final String TIMEINTERNAL = "timeinternal";

    public static final String KEYWORD = "keyword";

    public static final String SECRETKEY = "secretkey";

    public static final String TEXTTYPE = "msgtype";

    public static final String ATLink = "atlink";

    public static final String URL = "https://oapi.dingtalk.com/robot/send?access_token=";

    private String token;

    private String[] mobiles;

    /**
     * 去重过滤字段
     */
    private String[] distincts;

    /**
     * 时间间隔
     */
    private String timeInternal;

    /**
     * 告警关键词
     */
    private String keyWord;

    /**
     * 文本类型
     */

    private String textType;

    /**
     * 秘钥
     */
    private String secretKey;

    private String atLink;

    public String getAtLink() {
        return atLink;
    }

    public void setAtLink(String atLink) {
        this.atLink = atLink;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getTextType() {
        return textType;
    }

    public void setTextType(String textType) {
        this.textType = textType;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public String[] getDistincts() {
        return distincts;
    }

    public void setDistincts(String[] distincts) {
        this.distincts = distincts;
    }

    public String getTimeInternal() {
        return timeInternal;
    }

    public void setTimeInternal(String timeInternal) {
        this.timeInternal = timeInternal;
    }

    public DingdingSinkTableInfo() {
        setType(CURR_TYPE);
    }


    @Override
    public boolean check() {
        return true;
    }

    public static String getCurrType() {
        return CURR_TYPE;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String[] getMobiles() {
        return mobiles;
    }

    public void setMobiles(String[] mobiles) {
        this.mobiles = mobiles;
    }

    @Override
    public String toString() {
        return "DingdingSinkTableInfo{" +
                "token='" + token + '\'' +
                ", mobiles=" + Arrays.toString(mobiles) +
                ", distincts=" + Arrays.toString(distincts) +
                ", timeInternal='" + timeInternal + '\'' +
                ", keyWord='" + keyWord + '\'' +
                ", textType='" + textType + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", atLink='" + atLink + '\'' +
                '}';
    }
}
