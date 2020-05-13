package org.apache.atlas.notification;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Data;
import org.apache.atlas.model.notification.HookNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * cerate by JSQ
 */
@Data
public class KafkaMessageJson {

  private String queryId;
  private String queryStr;
  private String createTime;
  private String msgCreatedBy;
  private String executeAddress;
  private String type;
  private String operation;
  private String user;
  private String msgJson;
  private String failMessage;
  private String other;
  private String other2;

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageJson.class);


  public KafkaMessageJson(String msgJson, Object message, String address, String msgCreatedBy, Map<String, String> sqlInfo) {
    this.queryId = sqlInfo.get("queryId");
    this.queryStr = sqlInfo.get("queryStr");
    this.operation = sqlInfo.get("operation");
    this.failMessage = sqlInfo.get("failMessage");
    this.executeAddress = address;
    this.msgCreatedBy = msgCreatedBy;
    this.createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((new Date()).getTime());
    this.msgJson = msgJson;
    this.user = ((HookNotification)message).getUser();
    this.type = ((HookNotification)message).getType().toString();
    this.other = sqlInfo.get("other");
    this.other2 = "null";
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = mapper.createObjectNode();
    try {
      objectNode.put("queryId", queryId);
      objectNode.put("queryStr", queryStr);
      objectNode.put("createTime", createTime);
      objectNode.put("msgCreatedBy", msgCreatedBy);
      objectNode.put("executeAddress", executeAddress);
      objectNode.put("type", type);
      objectNode.put("operation", operation);
      objectNode.put("user", user);
      objectNode.put("msgJson", msgJson);
      objectNode.put("failMessage", failMessage);
      objectNode.put("other", other);
      objectNode.put("other2", other2);

    } catch (Exception e) {
      LOG.error("Failed to parse message to a json object. " + e);
    }
    return objectNode.toString();
  }

}
