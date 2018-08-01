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
  String response;
  String referer;
  String agent;

  @Override
  public double getBytes() {
    return this.bytes;
  }

  public String getAgent() {
    return this.agent;
  }

  public String getResponse() {
    return this.response;
  }

  public String getReferer() {
    return this.referer;
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

    String request = matcher.group(5).toLowerCase();
    String agent = matcher.group(9);
    CrawlerDetection crawlerDe = new CrawlerDetection(props);
    if (crawlerDe.checkKnownCrawler(agent)) {
      return null;
    } else {

      if (props.getProperty(MudrodConstants.REQUEST_LIST_STRATEGY).equals(MudrodConstants.WHITE)) {
        String[] searchPageTypes = props.getProperty(MudrodConstants.WHILE_LIST_REQUEST).split(",");
        boolean bContain = false;
        for (String searchType : searchPageTypes) {
          if (request.contains(searchType)) {
            bContain = true;
            break;
          }
        }
        if (!bContain) {
          return null;
        }
      } else {
        String[] mimeTypes = props.getProperty(MudrodConstants.BLACK_LIST_REQUEST).split(",");
        for (String mimeType : mimeTypes) {
          if (request.contains(mimeType)) {
            return null;
          }
        }
      }

      ApacheAccessLog accesslog = new ApacheAccessLog();
      accesslog.logType = MudrodConstants.HTTP_LOG;
      accesslog.IP = matcher.group(1);
      accesslog.request = matcher.group(5);
      accesslog.response = matcher.group(6);
      accesslog.bytes = Double.parseDouble(bytes);
      accesslog.referer = matcher.group(8);
      accesslog.agent = matcher.group(9);
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
      accesslog.time = df.format(date);

      return accesslog;
    }
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
      request = matcher.group(1);
      return baseUrl + request;
    }

    return request;
  }
}
