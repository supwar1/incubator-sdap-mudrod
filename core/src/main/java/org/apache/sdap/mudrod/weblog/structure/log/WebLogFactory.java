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
import org.apache.sdap.mudrod.weblog.pre.ImportLogFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * This class represents an FTP access log line.
 */
public class WebLogFactory extends WebLog implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ImportLogFile.class);
  
  public WebLogFactory() {
	  super();
  }

  public static WebLog parseFromLogLine(String log, Properties props){
    if(Character.isDigit(log.charAt(0))){
      return ApacheAccessLog.parseFromLogLine(log, props);
    }else{
      return FtpLog.parseFromLogLine(log,props);
    }    
  }
}
