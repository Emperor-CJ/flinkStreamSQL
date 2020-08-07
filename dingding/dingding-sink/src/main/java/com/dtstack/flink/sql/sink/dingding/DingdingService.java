package com.dtstack.flink.sql.sink.dingding;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import com.dtstack.flink.sql.sink.dingding.table.DingdingSinkTableInfo;
import com.taobao.api.ApiException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;

public class DingdingService implements Serializable {

    private String title;
    private Integer level;

    public void emit(DingdingSinkTableInfo dingdingSinkTableInfo, Row row) throws Exception {

        Integer index = Arrays.asList(dingdingSinkTableInfo.getFields()).indexOf("w_level");
        level = (Integer) row.getField(index);
        title = Level.levelTitleInfo(level);

        String boltUrl = DingdingSinkTableInfo.URL + dingdingSinkTableInfo.getToken();
        if (!StringUtils.isEmpty(dingdingSinkTableInfo.getSecretKey())) {
            String sc = SecretKeyUtil.createSecretKey(dingdingSinkTableInfo.getSecretKey());
            boltUrl = boltUrl + "&timestamp=" + sc.split("==")[1] + "&sign=" + sc.split("==")[0];
        }
        DingTalkClient client = new DefaultDingTalkClient(boltUrl);
        OapiRobotSendRequest request = new OapiRobotSendRequest();
        OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();

        /**
         * @用户
         */
        getLinkUser(dingdingSinkTableInfo, at, row);

        request.setAt(at);

        /**
         * 消息格式处理
         */
        dealMsgText(dingdingSinkTableInfo, request, row);


        OapiRobotSendResponse response = null;
        try {
            response = client.execute(request);
        } catch (ApiException e) {
            e.printStackTrace();
        }
        System.out.println(response.getBody());
    }

    private void dealMsgText(DingdingSinkTableInfo dingdingSinkTableInfo, OapiRobotSendRequest request, Row row) throws Exception {
        switch (dingdingSinkTableInfo.getTextType()) {
            case "text":
                testMsg(dingdingSinkTableInfo, request, row);
                break;
            case "markdown":
                markdownMsg(dingdingSinkTableInfo, request, row);
                break;
            default:
                throw new Exception("暂不支持" + dingdingSinkTableInfo.getTextType() + "文本类型");
        }
    }

    private void testMsg(DingdingSinkTableInfo dingdingSinkTableInfo, OapiRobotSendRequest request, Row row) {
        request.setMsgtype("text");
        OapiRobotSendRequest.Text text = new OapiRobotSendRequest.Text();
        String content = msg(dingdingSinkTableInfo, row);
        text.setContent(content);
        request.setText(text);
    }

    private void markdownMsg(DingdingSinkTableInfo dingdingSinkTableInfo, OapiRobotSendRequest request, Row row) {
        request.setMsgtype("markdown");
        OapiRobotSendRequest.Markdown markdown = new OapiRobotSendRequest.Markdown();
        markdown.setTitle(title);
        markdown.setText("#### 杭州天气 @156xxxx8827\n" +
                "> 9度，西北风1级，空气良89，相对温度73%\n\n" +
                "> ![screenshot](https://gw.alicdn.com/tfs/TB1ut3xxbsrBKNjSZFpXXcXhFXa-846-786.png)\n" +
                "> ###### 10点20分发布 [天气](http://www.thinkpage.cn/) \n");
        request.setMarkdown(markdown);
    }

    private String msg(DingdingSinkTableInfo dingdingSinkTableInfo, Row row) {
        StringBuffer sb = new StringBuffer();
        sb.append(title).append("\n");
        for (int i = 0; i < dingdingSinkTableInfo.getFields().length; i++) {
            if (dingdingSinkTableInfo.getFields()[i].contains("w_level")) {
                continue;
            }
            String fieldName = dingdingSinkTableInfo.getFields()[i];
            String key = (String) dingdingSinkTableInfo.getNameMap().getOrDefault(fieldName, fieldName);
            if (fieldName.contains("w_host") || fieldName.contains("w_time")) {
                key = Level.levelInfo(level) + key;
            }
            sb.append(key).append(" : ").append(row.getField(i)).append("\n");
        }

        return sb.toString();
    }


    private void getLinkUser(DingdingSinkTableInfo dingdingSinkTableInfo, OapiRobotSendRequest.At at, Row row) {
        if (dingdingSinkTableInfo.getMobiles() == null || dingdingSinkTableInfo.getMobiles().length == 0) {
            at.setIsAtAll(true);
        } else if (dingdingSinkTableInfo.getMobiles().length == 1 && Arrays.asList(dingdingSinkTableInfo.getFields()).contains(dingdingSinkTableInfo.getMobiles()[0])) {
            Integer index = Arrays.asList(dingdingSinkTableInfo.getFields()).indexOf(dingdingSinkTableInfo.getMobiles()[0]);
            String link = (String) row.getField(index);
            if (StringUtils.isEmpty(link)) {
                at.setIsAtAll(true);
            } else {
                try {
                    String[] s = link.split(" ");
                    String mobile = s[0].split("-")[1].trim();
                    at.setAtMobiles(Arrays.asList(mobile));
                    at.setIsAtAll(false);
                }catch (Exception e){
                    at.setIsAtAll(true);
                }
            }
        } else {
            at.setAtMobiles(Arrays.asList(dingdingSinkTableInfo.getMobiles()));
            at.setIsAtAll(false);
        }
    }

}
