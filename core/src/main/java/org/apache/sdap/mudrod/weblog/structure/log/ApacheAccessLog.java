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

import org.apache.sdap.mudrod.main.MudrodConstants;
import java.io.Serializable;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */

public class ApacheAccessLog extends HttpLog implements Serializable {
  
  public ApacheAccessLog() {
	  super();
	  this.LogType = MudrodConstants.ACCESS_LOG;
  }
  
  @Override
  public String getURL() {
	  return MudrodConstants.ACCESS_URL;
  }


}
