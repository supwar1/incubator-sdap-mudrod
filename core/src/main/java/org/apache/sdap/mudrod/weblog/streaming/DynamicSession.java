package org.apache.sdap.mudrod.weblog.streaming;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.weblog.structure.log.ApacheAccessLog;
import org.apache.sdap.mudrod.weblog.structure.log.RequestUrl;
import org.apache.sdap.mudrod.weblog.structure.log.WebLog;
import org.apache.sdap.mudrod.weblog.structure.session.ClickStream;
import org.apache.sdap.mudrod.weblog.structure.session.RankingTrainData;
import org.apache.sdap.mudrod.weblog.structure.session.SessionNode;
import org.apache.sdap.mudrod.weblog.structure.session.SessionTree;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.clearspring.analytics.stream.membership.Filter;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DynamicSession implements Serializable {

  private String start_time;
  private String end_time;
  private String ip;
  private String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'";
  private List<WebLog> logList = new LinkedList<>();
  private SessionTree tree = null;
  private Properties props;
  
  private List<ClickStream> clickStream;
  private List<RankingTrainData> trainData;
  
  public DynamicSession(WebLog log, Properties props) {
    this.start_time = log.getTime();
    this.end_time = log.getTime();
    this.ip = log.getIP();
    this.logList.add(log);
    this.props = props;
    
    this.tree = new SessionTree(props, null, this.ip, "dynamic");
    
    String request = log.getRequest();
    String time = log.getTime();
    String logType = log.getLogType();
    String referer = "";
    if(logType.equals(MudrodConstants.ACCESS_LOG)){
       referer = ((ApacheAccessLog)log).getReferer();
       request = ((ApacheAccessLog)log).parseUrl(request, props.getProperty(MudrodConstants.ACCESS_URL));
    }
    
    SessionNode node = new SessionNode(props, request, logType, referer, time, 1);
    if (MudrodConstants.SEARCH_MARKER.equals(node.getKey()))
      this.tree.insert(node);
  }

  public DynamicSession(String start_time, String end_time, String ip, List<WebLog> list, Properties props) {
    this.start_time = start_time;
    this.end_time = end_time;
    this.ip = ip;
    this.logList = list;
    this.props = props;
    
    this.tree = new SessionTree(props, null, this.ip, "dynamic");
    
    for (WebLog log: list) {
      String request = log.getRequest();
      String time = log.getTime();
      String logType = log.getLogType();
      String referer = "";
      if(logType.equals(MudrodConstants.ACCESS_LOG)){
         referer = ((ApacheAccessLog)log).getReferer();
         request = ((ApacheAccessLog)log).parseUrl(request, props.getProperty(MudrodConstants.ACCESS_LOG));
      }
      
      SessionNode node = new SessionNode(props, request, logType, referer, time, 1);
      if (MudrodConstants.SEARCH_MARKER.equals(node.getKey()))
        this.tree.insert(node);
    }
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
    // test
    System.out.print("*Reduce state ");
    if (s1 != null) {
      System.out.print(s1.getIpAddress() + " ");
    }
    if (s2 != null) {
      System.out.println(s2.getIpAddress());
    }
    
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

      s = new DynamicSession(new_start_time.toString(formatter), new_end_time.toString(formatter), s2.getIpAddress(), new_logList, s1.props);
    }

    return s;
  }
  
  public void incorporate(DynamicSession newSession) {
    // why consider time stamp? they come in order
    // check ip?
    for (WebLog log: newSession.getLogList()) {
      this.logList.add(log);
      String request = log.getRequest();
      String time = log.getTime();
      String logType = log.getLogType();
      String referer = "";
      if(logType.equals(MudrodConstants.ACCESS_LOG)){
         referer = ((ApacheAccessLog)log).getReferer();
         request = ((ApacheAccessLog)log).parseUrl(request, props.getProperty(MudrodConstants.ACCESS_URL));
      }
      
      SessionNode node = new SessionNode(props, request, logType, referer, time, 1);
      this.tree.insert(node);

    }
  }
  
  

  public boolean hasHttpLog() {
    boolean hasHttp = false;
    for (WebLog log : logList) {
      if (log.getLogType().equals(MudrodConstants.ACCESS_LOG)) {
        hasHttp = true;
        break;
      }
    }
    return hasHttp;
  }
  
  public void parseLogs() {
    RequestUrl requestURL = new RequestUrl();
    for (WebLog log : logList) {
      if (log.getLogType().equals(MudrodConstants.ACCESS_LOG)) {
        String queryUrl = log.getRequest();
        if(queryUrl.contains("datasetlist")){
          String searchStr = requestURL.getSearchWord(queryUrl);
          Map<String, String> filter = null;
          try {
            filter = RequestUrl.getFilterInfo(queryUrl);
          } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          
          String searchInfo = "";
          if(!searchStr.equals("")){
            searchInfo += "search:" + searchStr + " | " ;
          }
          
          if(filter != null && filter.size()>0){
            for(String field: filter.keySet()){
              searchInfo += field + ":" + filter.get(field) + " | ";
            }
          }
          
          if(!searchInfo.equals("")){
            System.out.println("search behaviour: " + searchInfo);
          }
        }
        else{
          String dataset = queryUrl.replace("GET /dataset?", "");
          System.out.println("click behaviour: " + dataset);
        }
      }else{
        System.out.println("download behaviour");
      }
    }
  }
  
  public SessionTree buildTree(Properties props) throws UnsupportedEncodingException {
    tree = new SessionTree(props, null, "", "");
    int seq = 1;
    for (WebLog log: logList) {
     
      String request = log.getRequest();
      String time = log.getTime();
      String logType = log.getLogType();
      String referer = "";
      if(logType.equals(MudrodConstants.ACCESS_LOG)){
         referer = ((ApacheAccessLog)log).getReferer();
         request = ((ApacheAccessLog)log).parseUrl(request, props.getProperty(MudrodConstants.ACCESS_URL));
      }
      
      SessionNode node = new SessionNode(props, request, logType, referer, time, seq);
      tree.insert(node);
      seq++;
    }
    
    tree.printTree(tree.getRoot());

    return tree;
  }

  /**
   * Obtain the ranking training data.
   *
   * @param indexName
   *            the index from whcih to obtain the data
   * @param sessionID
   *            a valid session identifier
   * @return {@link ClickStream}
   * @throws UnsupportedEncodingException
   *             if there is an error whilst processing the ranking training
   *             data.
   */
  public List<RecomTrainData> getRecomTrainData(SessionTree tree)
      throws UnsupportedEncodingException {

    List<RecomTrainData> trainDatas = new ArrayList<>();

    List<SessionNode> branches = tree.getRoot().getChildren();
    for (int i = 0; i < branches.size(); i++) {
      SessionNode branch = branches.get(i);
      List<SessionNode> branchDataNodes = tree.getNodes(branch, "dataset");
      if(branchDataNodes.size() <= 1){ //important
        continue;
      }
      
      List<String> queries = new ArrayList<String>();
      List<Map<String, String>> filters = new ArrayList<Map<String, String>>();
      List<String> datasets = new ArrayList<String>();
      List<Boolean> bDownloads = new ArrayList<Boolean>();
      List<String> requests = new ArrayList<String>(); 
      
      for (int j = 0; j < branchDataNodes.size(); j++) {
        SessionNode datanode = branchDataNodes.get(j);
        String datasetId = datanode.getDatasetId();
        boolean bDownload = false;
        List<SessionNode> dataChildren = datanode.getChildren();
        int childSize = dataChildren.size();
        for (int k = 0; k < childSize; k++) {
          if (MudrodConstants.FTP_LOG.equals(dataChildren.get(k).getKey())) {
            bDownload = true;
            break;
          }
        }

        RequestUrl requestURL = new RequestUrl();
        String queryUrl = datanode.getRequest();
        String searchStr = requestURL.getSearchWord(queryUrl);
        /*String query = null;
        try {
          query = es.customAnalyzing(props.getProperty("indexName"), searchStr);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException("Error performing custom analyzing", e);
        }*/
        Map<String, String> filter = RequestUrl.getFilterInfo(queryUrl);
        
        datasets.add(datasetId);
        bDownloads.add(bDownload);
        queries.add(searchStr);
        filters.add(filter);  
        requests.add(queryUrl);
      }
      
      RecomTrainData trainData = new RecomTrainData(datasets, queries, filters, bDownloads);
      //trainData.setIndex(indexName);
      //trainData.setSessionId(sessionID);
      trainData.setRequests(requests);
      trainDatas.add(trainData);
    }
    
    return trainDatas;
  }
  
  public JsonObject getSessionDetail() {
    JsonObject sessionResults = new JsonObject();
    // for session tree

    JsonObject jsonTree = this.tree.treeToJson(this.tree.getRoot());
    sessionResults.add("treeData", jsonTree);

    return sessionResults;
  }
  
  
  // test only
  public void printSessionTree() {
    this.tree.printTree(this.tree.getRoot());
    System.out.println(this.getSessionDetail());
  }
  
  public void generateClickStream() throws UnsupportedEncodingException {
    clickStream = this.tree.getClickStreamList(props);
  }
  
  public void printClickStream() {
    for (int i = 0; i < clickStream.size(); i++) {
      System.out.println(clickStream.get(i).toString());
    }
  }
  
  public void generateRankingTrainData() throws UnsupportedEncodingException {
    this.trainData = this.tree.getRankingTrainData("dynamic");
  }
  
  public void printRankingTrainData() {
    for (int i = 0; i < trainData.size(); i++) {
      System.out.println(trainData.get(i).toString());
    }
  }
  
}
