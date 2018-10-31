package org.apache.sdap.mudrod.weblog.streaming;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class PrepareStreamData {

  
  public int generateData(ESDriver es) {

    String index = "log201502.w1.gz";
    String cleanupType = "cleanup.log";
    
    String httpName = "E://data//mudrod//stream_source//HTTP";
    String ftpName = "E://data//mudrod//stream_source//FTP";
    
    File httpFile = new File(httpName);
    if (httpFile.exists()) {
      httpFile.delete();
    }
    
    File ftpFile = new File(ftpName);
    if (ftpFile.exists()) {
      ftpFile.delete();
    }
    
    try {
      httpFile.createNewFile();
      ftpFile.createNewFile();
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    
    FileWriter httpfw= null;
    BufferedWriter httpbw = null;
    FileWriter ftpfw= null;
    BufferedWriter ftpbw = null;
    try {
      httpfw = new FileWriter(httpFile.getAbsoluteFile());
      httpbw = new BufferedWriter(httpfw);
      
      ftpfw = new FileWriter(ftpFile.getAbsoluteFile());
      ftpbw = new BufferedWriter(ftpfw);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    SearchResponse scrollResp = es.getClient()
            .prepareSearch(index)
            .setTypes(cleanupType)
            .setScroll(new TimeValue(60000))
            .addSort("Time", SortOrder.ASC)
            .setSize(100)
            .execute()
            .actionGet();

    while (scrollResp.getHits().getHits().length != 0) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String logType = (String) result.get("LogType");
        String log = (String) result.get("log");
        String sessionID = (String) result.get("SessionID");
        if(sessionID.equals("invalid")){
          continue;
        }
        if (logType.equals(MudrodConstants.HTTP_LOG)) {
          
          try {
            httpbw.write(log + "\n");
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }else{
          try {
            ftpbw.write(log + "\n");
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
       
      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
    }
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {

     MudrodEngine mudrod = new MudrodEngine();
     Properties props = mudrod.loadConfig();
     SparkDriver spark = new SparkDriver(props);
     
     ESDriver es = mudrod.startESDriver();

     PrepareStreamData monitor = new PrepareStreamData();
     monitor.generateData(es);
   }
}
