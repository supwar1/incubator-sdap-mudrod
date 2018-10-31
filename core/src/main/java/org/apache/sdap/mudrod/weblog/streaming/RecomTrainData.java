package org.apache.sdap.mudrod.weblog.streaming;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * ClassName: train data extracted from web logs for training ranking weightss.
 */
public class RecomTrainData implements Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	// sessionID: session ID
	private String sessionID;
	// type: session type name
	private String index;
	// query: query words related to the click
	private List<String> queries;
	private List<Map<String, String>> filters;
	private List<String> datasets;
	private List<Boolean> bDownloads;
	private List<String> requests;

	/**
	 * Creates a new instance of ClickStream.
	 *
	 * @param query
	 *            the user query string
	 * @param highRankDataset
	 *            the dataset name for the highest ranked dataset
	 * @param lowRankDataset
	 *            the dataset name for the lowest ranked dataset
	 */
	public RecomTrainData(List<String> datasets, List<String> queries, List<Map<String, String>> filters,
			List<Boolean> bDownloads) {
		this.queries = queries;
		this.datasets = datasets;
		this.filters = filters;
		this.bDownloads = bDownloads;
	}

	public RecomTrainData() {
		// default constructor
	}

	public String getSessionID() {
		return sessionID;
	}

	/**
	 * setSessionId: Set ID of session
	 *
	 * @param sessionID
	 *            session id
	 */
	public void setSessionId(String sessionID) {
		this.sessionID = sessionID;
	}
	
	public void setRequests(List<String> requests) {
		this.requests = requests;
	}

	/**
	 * setType: Set session type name
	 *
	 * @param index
	 *            session type name in elasticsearch
	 */
	public void setIndex(String index) {
		this.index = index;
	}

	/**
	 * Output click stream info in string format
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		// return "query:" + query + "|| highRankDataset:" + highRankDataset +
		// "|| lowRankDataset:" + lowRankDataset;
		return "";
	}

	/**
   * toJson: Output click stream info in Json format
   *
   * @return session in string format
   */
  public String toJson() {
    String jsonQuery = "{";
    jsonQuery += "\"sessionId\":\"" + this.sessionID + "\",";
    jsonQuery += "\"index\":\"" + this.index + "\",";
    
    int size = datasets.size();
    jsonQuery += "[";
    for(int i=0; i<size; i++){
    	jsonQuery += "{";
    	
    	String query = queries.get(i);
    	String dataset = datasets.get(i);
    	Boolean bDownload = bDownloads.get(i);
    	Map<String, String> filter = filters.get(i);
    	String request = this.requests.get(i);
    	
    	jsonQuery += "\"query\":\"" + query + "\",";
        jsonQuery += "\"dataset\":\"" + dataset + "\",";
        jsonQuery += "\"download\":\"" + bDownload+ "\",";
        jsonQuery += "\"request\":\"" + request+ "\",";
        if (filter != null) {
          for (String key : filter.keySet()) {
            jsonQuery += "\"" + key + "\":\"" + filter.get(key) + "\",";
          }
        }
        
        jsonQuery += "},";
        
    }
    jsonQuery += "]";
    jsonQuery += "}";
    return jsonQuery;
  }
}
