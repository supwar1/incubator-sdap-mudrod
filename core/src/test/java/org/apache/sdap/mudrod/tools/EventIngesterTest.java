package org.apache.sdap.mudrod.tools;

import static org.apache.sdap.mudrod.main.MudrodConstants.DATA_DIR;
import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.ontology.process.LocalOntology;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class EventIngesterTest {

  private EventIngester ingester;
  private static MudrodEngine engine = new MudrodEngine();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    engine.loadConfig();
    engine.setESDriver(engine.startESDriver());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    engine.setESDriver(null);
    engine = null;
  }

  @Before
  public void setUp() throws Exception {
    ingester = new EventIngester(engine.getConfig(), engine.getESDriver(), null);
  }

  @After
  public void tearDown() throws Exception {
    
    
    ingester = null;
  }

  @Test
  public void testEventIngester() {
    assertNotNull("Test setUp should create a new instance of EventIngester.", ingester);
  }

  @Test
  public void testIngestAllEonetEvents() {
    String result = ingester.ingestAllEonetEvents(engine);
    assertTrue("Event ingester should have ingested at least one event", !result.equals(""));
  }

  @Test
  public void testIngestEventsJSON() {
    fail("Not yet implemented");
  }
}
