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
package org.apache.sdap.mudrod.services.log;

import org.apache.commons.cli.HelpFormatter;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * A Dataset Detail Search Resource
 */
@Path("/offlinelog")
public class OfflineLog {

  private static final Logger LOG = LoggerFactory.getLogger(OfflineLog.class);
  private MudrodEngine mEngine;

  public OfflineLog(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
  }

  /**
   * A simple health status checker for this resource.
   *
   * @return a static html response if the service is running correctly.
   */
  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response.ok("<h1>This is MUDROD offline log ingest Resource: running correctly...</h1>").build();
  }

  @GET
  @Path("/ingest")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response ingest(@QueryParam("logPath") String logPath) {
    Properties config = mEngine.getConfig();
    String result = null;
    
    try {
      mEngine.setProperty(MudrodConstants.DATA_DIR, logPath);
      mEngine.startLogIngest();
      result = "sucessfully ingest logs";
    }catch (Exception e) {
      result = "error while ingesting logs." + e;
    }
    
    String json = new Gson().toJson(result);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }
}
