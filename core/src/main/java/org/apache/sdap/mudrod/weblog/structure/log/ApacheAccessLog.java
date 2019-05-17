/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sdap.mudrod.weblog.structure.log;

import com.google.gson.Gson;

import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.weblog.pre.CrawlerDetection;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog extends WebLog implements Serializable {
  String Response;
  String Referer;
  String Browser;

  @Override
  public double getBytes() {
    return this.Bytes;
  }

  public String getBrowser() {
    return this.Browser;
  }

  public String getResponse() {
    return this.Response;
  }

  public String getReferer() {
    return this.Referer;
  }

  public ApacheAccessLog() {
    super();
  }

  public static ApacheAccessLog parseFromLogLine(String log, Properties props) {

    String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";
    final int numFields = 9;
    Pattern p = Pattern.compile(logEntryPattern);
    Matcher matcher;

    matcher = p.matcher(log);
    if (!matcher.matches() || numFields != matcher.groupCount()) {
      return null;
    }

    String time = matcher.group(4);
    time = SwithtoNum(time);
    SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
    Date date = null;
    try {
      date = formatter.parse(time);
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    String bytes = matcher.group(7);

    if ("-".equals(bytes)) {
      bytes = "0";
    }
    String type = "";
    if (matcher.group(5).contains("GET /opendap")) {
      type = MudrodConstants.OPENDAP_LOG;
    } else if (matcher.group(5).contains("GET /thredds")) {
      type = MudrodConstants.THREDDS_LOG;
    } else {
      type = MudrodConstants.ACCESS_LOG;
    }
    String request = matcher.group(5).toLowerCase();
    String agent = matcher.group(9);
    CrawlerDetection crawlerDe = new CrawlerDetection(props);
    if (crawlerDe.checkKnownCrawler(agent)) {
      return null;
    } else {
      /*
       * if (props.getProperty(MudrodConstants.REQUEST_LIST_STRATEGY).equals(
       * MudrodConstants.WHITE)) { String[] searchPageTypes =
       * props.getProperty(MudrodConstants.WHILE_LIST_REQUEST).split(",");
       * boolean bContain = false; for (String searchType : searchPageTypes) {
       * if (request.contains(searchType.trim())) { bContain = true; break; } }
       * if (!bContain) { return null; } } else {
       */
      String[] mimeTypes = props.getProperty(MudrodConstants.BLACK_LIST_REQUEST).split(",");
      for (String mimeType : mimeTypes) {
        if (request.contains(mimeType)) {
          return null;
        }
      }
    }

    ApacheAccessLog accesslog = new ApacheAccessLog();
    accesslog.LogType = type;
    accesslog.IP = matcher.group(1);
    accesslog.Request = matcher.group(5);
    accesslog.Response = matcher.group(6);
    accesslog.Bytes = Double.parseDouble(bytes);
    accesslog.Referer = matcher.group(8);
    accesslog.Browser = matcher.group(9);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
    accesslog.Time = df.format(date);

    accesslog.log = log; // for test
    
    if (accesslog.Referer.length() >= 31 && accesslog.Referer.substring(0, 4).equals("http")) {
      if (accesslog.Referer.charAt(4) != 's') {
        accesslog.Referer = accesslog.Referer.replaceFirst("http", "https");
      }
      if (accesslog.Referer.substring(8, 15).equals("podaac-")) {
        // https://podaac-www.jpl.nasa.gov/dataaccess
        // https://podaac-ftp.jpl.nasa.gov/dataaccess
        if (accesslog.Referer.substring(15, 18).equals("www")) {
          accesslog.Referer = accesslog.Referer.replaceFirst("-www", "");
        } else if (accesslog.Referer.substring(15, 18).equals("ftp")) {
          accesslog.Referer = accesslog.Referer.replaceFirst("-ftp", "");
        } else if (accesslog.Referer.substring(15, 22).equals("opendap")) {
          // https://podaac-opendap.jpl.nasa.gov/opendap/allData/aquarius/L3/mapped/V5/7day_running/SCI/2014/contents.html
          accesslog.Referer = accesslog.Referer.replaceFirst("podaac-", "");
        }
      }
    }

    return accesslog;
  }


  public static String parseFromLogLineToJson(String log, Properties props) throws IOException, ParseException {

    ApacheAccessLog accesslog = ApacheAccessLog.parseFromLogLine(log, props);
    if (accesslog == null) {
      return "{}";
    }
    Gson gson = new Gson();
    String lineJson = gson.toJson(accesslog);

    return lineJson;
  }

  public static boolean checknull(WebLog s) {
    if (s == null) {
      return false;
    }
    return true;
  }

  public String parseUrl(String request, String baseUrl) {
    Pattern pattern = Pattern.compile("get (.*?) http/*");
    Matcher matcher;
    matcher = pattern.matcher(request.trim().toLowerCase());
    while (matcher.find()) {

      // request = matcher.group(1);
      // return baseUrl + request;
      return matcher.group(1);
    }

    return null;
  }
}
