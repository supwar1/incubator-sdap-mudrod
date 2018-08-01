package org.apache.sdap.mudrod.weblog.streaming;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.weblog.structure.log.ApacheAccessLog;
import org.apache.sdap.mudrod.weblog.structure.log.WebLog;
import org.apache.sdap.mudrod.weblog.structure.session.SessionNode;
import org.apache.sdap.mudrod.weblog.structure.session.SessionTree;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DynamicSession implements Serializable {

  private String start_time;
  private String end_time;
  private String ip;
  private String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'";
  private List<WebLog> logList = new LinkedList<>();
  private SessionTree tree = null;
  
  public DynamicSession(WebLog log) {
    this.start_time = log.getTime();
    this.end_time = log.getTime();
    this.ip = log.getIP();
    this.logList.add(log);
  }

  public DynamicSession(String start_time, String end_time, String ip, List<WebLog> list) {
    this.start_time = start_time;
    this.end_time = end_time;
    this.ip = ip;
    this.logList = list;
  }

  public String getStartTime() {
    return start_time;
  }

  public String getEndTime() {
    return end_time;
  }

  public DateTime getStartTimeObj() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(this.dateFormat);
    DateTime start_time_obj = formatter.withOffsetParsed().parseDateTime(this.start_time).toLocalDateTime().toDateTime();

    return start_time_obj;
  }

  public DateTime getEndTimeObj() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(this.dateFormat);
    DateTime end_time_obj = formatter.withOffsetParsed().parseDateTime(this.end_time).toLocalDateTime().toDateTime();
    return end_time_obj;
  }

  public String getIpAddress() {
    return ip;
  }

  public List<WebLog> getLogList() {
    return logList;
  }

  public void setStartTime(String time) {
    start_time = time;
  }

  public void setEndTime(String time) {
    end_time = time;
  }

  public void setIpAddress(String ipString) {
    ip = ipString;
  }

  public static DynamicSession add(DynamicSession s1, DynamicSession s2) {
    DynamicSession s = null;
    if (s1 == null && s2 != null) {
      s = s2;
    } else if (s1 != null && s2 == null) {
      s = s1;
    } else if (s1 != null && s2 != null) {
      DateTimeFormatter formatter = DateTimeFormat.forPattern(s1.dateFormat);

      DateTime new_start_time = s1.getStartTimeObj();
      DateTime new_end_time = s1.getEndTimeObj();
      List<WebLog> new_logList = new ArrayList<>();

      if (new_start_time.isAfter(s2.getStartTimeObj())) {
        new_start_time = s2.getStartTimeObj();
      }

      if (new_end_time.isBefore(s2.getEndTimeObj())) {
        new_end_time = s2.getEndTimeObj();
      }

      new_logList.addAll(s1.getLogList());
      new_logList.addAll(s2.getLogList());

      s = new DynamicSession(new_start_time.toString(formatter), new_end_time.toString(formatter), s2.getIpAddress(), new_logList);
    }

    return s;
  }
  
  public static DynamicSession add(DynamicSession s1, DynamicSession s2, Properties props) {
    DynamicSession s = DynamicSession.add(s1,s2);

    try {
      s.buildTree(props);
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return s;
  }


  public boolean hasHttpLog() {
    boolean hasHttp = false;
    for (WebLog log : logList) {
      if (log.getLogType().equals(MudrodConstants.HTTP_LOG)) {
        hasHttp = true;
        break;
      }
    }
    return hasHttp;
  }
  
  public SessionTree buildTree(Properties props) throws UnsupportedEncodingException {
    tree = new SessionTree(props, null, "", "");
    int seq = 1;
    for (WebLog log: logList) {
     
      String request = log.getRequest();
      String time = log.getTime();
      String logType = log.getLogType();
      String referer = "";
      if(logType.equals(MudrodConstants.HTTP_LOG)){
         referer = ((ApacheAccessLog)log).getReferer();
         request = ((ApacheAccessLog)log).parseUrl(request, props.getProperty(MudrodConstants.BASE_URL));
      }
      
      SessionNode node = new SessionNode(props, request, logType, referer, time, seq);
      tree.insert(node);
      seq++;
    }

    return tree;
  }
}
