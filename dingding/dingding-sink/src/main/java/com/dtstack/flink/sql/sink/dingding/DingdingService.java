package com.dtstack.flink.sql.sink.dingding;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import com.dtstack.flink.sql.sink.dingding.table.DingdingSinkTableInfo;
import com.taobao.api.ApiException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DingdingService implements Serializable {

    private List<String> links;

    public synchronized void emit(DingdingSinkTableInfo dingdingSinkTableInfo, Row row, List<String> linkList) throws Exception {

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
        getLinkUser(dingdingSinkTableInfo, at, row, linkList);

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

        String res = response.getBody();
        System.out.println("res-body" + res);
        if ((Integer) JSONObject.parseObject(res).get("errcode") == 0) {
            if (dingdingSinkTableInfo.getTextType().equals("markdown") && links != null && links.size() != 0) {
                DingdingService dingdingService = new DingdingService();
                DingdingSinkTableInfo dingdingSinkTableInfo1 = ObjClonerSeiz.CloneObj(dingdingSinkTableInfo);
                dingdingSinkTableInfo1.setTextType("text");
                dingdingService.emit(dingdingSinkTableInfo1, Row.of("请及时关注"), links);
            }
        }
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
        text.setContent(msg(row));
        request.setText(text);
    }

    private void markdownMsg(DingdingSinkTableInfo dingdingSinkTableInfo, OapiRobotSendRequest request, Row row) throws InterruptedException {
        request.setMsgtype("markdown");
        OapiRobotSendRequest.Markdown markdown = new OapiRobotSendRequest.Markdown();
        markdown.setTitle(dingdingSinkTableInfo.getKeyWord());
        markdown.setText(msg(row));
        request.setMarkdown(markdown);

    }

    private String msg(Row row) {
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < row.getArity(); i++) {
            sb.append(row.getField(i)).append(" \n");
        }

        return sb.toString();
    }


    private void getLinkUser(DingdingSinkTableInfo dingdingSinkTableInfo, OapiRobotSendRequest.At at, Row row, List<String> linkList) {
        links = new ArrayList<>();
        if (dingdingSinkTableInfo.getMobiles() == null || dingdingSinkTableInfo.getMobiles().length == 0) {
            System.out.println("0");
            at.setIsAtAll(true);
        } else if (dingdingSinkTableInfo.getMobiles().length == 1 && Arrays.asList(dingdingSinkTableInfo.getFields()).contains(dingdingSinkTableInfo.getMobiles()[0])) {
            Integer index = Arrays.asList(dingdingSinkTableInfo.getFields()).indexOf(dingdingSinkTableInfo.getMobiles()[0]);
            if (linkList != null && linkList.size() != 0) {
                System.out.println("1");
                at.setAtMobiles(linkList);
                at.setIsAtAll(false);
            } else {
                try {
                    String link = (String) row.getField(index);
                    Pattern p = Pattern.compile("1[345678]\\d{9}");
                    Matcher m = p.matcher(link);

                    while (m.find()) {
                        links.add(m.group());
                    }
                    at.setAtMobiles(links);
                    at.setIsAtAll(false);
                } catch (Exception e) {
                    at.setIsAtAll(true);
                }
            }
        } else {
            if (linkList != null && linkList.size() != 0) {
                at.setAtMobiles(linkList);
            } else {
                links = Arrays.asList(dingdingSinkTableInfo.getMobiles());
                at.setAtMobiles(links);
            }
            at.setIsAtAll(false);
        }
    }

}
