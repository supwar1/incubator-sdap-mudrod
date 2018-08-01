package org.apache.sdap.mudrod.weblog.streaming;

import java.util.List;
import java.util.Properties;

import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.weblog.structure.log.WebLog;
import org.apache.sdap.mudrod.weblog.structure.log.WebLogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import scala.Tuple2;
import org.joda.time.Seconds;

/**
 * The LogAnalyzerImportStreamingFile illustrates how to run Spark Streaming,
 * but instead of monitoring a socket, it monitors a directory and feeds in any
 * new files to streaming.
 *
 * Once you get this program up and running, feed apache access log files into
 * that directory.
 *
 * Example command to run: % ${YOUR_SPARK_HOME}/bin/spark-submit --class
 * "com.databricks.apps.logs.chapter2.LogAnalyzerStreamingImportDirectory"
 * --master spark://YOUR_SPARK_MASTER YOUR_LOCAL_LOGS_DIRECTORY
 * target/log-analyzer-1.0.jar
 */
public class LogMonitor {
  private static Properties props;
  private static final Duration SLIDE_INTERVAL = new Duration(3 * 1000);
  private static final int interval_timeout = 30;

  private static final Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
  private static final Function2<DynamicSession, DynamicSession, DynamicSession> Session_Merger = (s1, s2) -> DynamicSession.add(s1, s2);
  private static final Function2<List<DynamicSession>, Optional<DynamicSession>, Optional<DynamicSession>> COMPUTE_RUNNING_SESSION = (news, current) -> {
    DynamicSession s = current.orNull();
    DateTime now = DateTime.now().toLocalDateTime().toDateTime();
    if (s != null) {
      int interval = Seconds.secondsBetween(s.getEndTimeObj(), now).getSeconds();
      if (interval > interval_timeout) {
          s = null;
      }
    } 
    
    for (DynamicSession i : news) {
      s = DynamicSession.add(s, i);
    }

    if (s == null || !s.hasHttpLog()) {
      return Optional.absent();
    } else {
      return Optional.of(s);
    }
  };

  private static final Function2<List<DynamicSession>, Optional<DynamicSession>, Optional<DynamicSession>> COMPUTE_RUNNING_SESSION_BAK = (news, current) -> {
    DynamicSession s = current.orNull();
    DateTime now = DateTime.now().toLocalDateTime().toDateTime(); // change the
                                                                  // value of
                                                                  // now to test
    if (s != null) {
      int interval = Seconds.secondsBetween(s.getEndTimeObj(), now).getSeconds();
      if (interval > interval_timeout) {
        if (news.size() > 0) {
          for (DynamicSession i : news) {
            s = DynamicSession.add(null, i);
          }
          return Optional.of(s);
        } else {
          return Optional.absent();
        }
      }
    }

    for (DynamicSession i : news) {
      s = DynamicSession.add(s, i);
    }
    return Optional.of(s);
  };

  public void monitorLog(SparkDriver spark, Properties props, String directory) {

    this.props = props;
    JavaStreamingContext jssc = new JavaStreamingContext(spark.sc, SLIDE_INTERVAL);
    jssc.checkpoint("checkpoints-mudrod-streaming-total");

    JavaDStream<String> logDataDStream = jssc.textFileStream(directory);
    JavaDStream<WebLog> usefulLogDStream = logDataDStream.map(log -> WebLogFactory.parseFromLogLine(log, props)).filter(log -> log != null);

    // A DStream of sessions with ip being the key
    JavaPairDStream<String, DynamicSession> ipDStream = usefulLogDStream.mapToPair(s -> new Tuple2<>(s.getIP(), new DynamicSession(s))).reduceByKey(Session_Merger)
        .updateStateByKey(COMPUTE_RUNNING_SESSION);

    ipDStream.foreachRDD(rdd -> {
      List<Tuple2<String, DynamicSession>> sessions = rdd.take(100);
      for (Tuple2<String, DynamicSession> t : sessions) {
        List<WebLog> logs = t._2.getLogList();
        int httpCount = 0;
        int ftpCount = 0;
        for (WebLog log : logs) {
          if (log.getLogType().equals(MudrodConstants.HTTP_LOG)) {
            httpCount += 1;
          } else {
            ftpCount += 1;
          }
        }
        System.out.println(t._1 + " " + httpCount + " http, " + ftpCount + " ftp");
        
        String click = t._2.buildTree(props).getClickStreamList(props).toString();
        System.out.println(click);
      }
      System.out.println("***************");
    });

    // Start the streaming server.
    jssc.start();
    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("Must specify an access logs directory.");
      System.exit(-1);
    }

    String directory = args[0];

    MudrodEngine mudrod = new MudrodEngine();
    Properties props = mudrod.loadConfig();
    SparkDriver spark = new SparkDriver(props);

    LogMonitor monitor = new LogMonitor();
    monitor.monitorLog(spark, props, directory);
  }
}
