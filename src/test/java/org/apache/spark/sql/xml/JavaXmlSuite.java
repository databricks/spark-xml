//package org.apache.spark.sql.xml;
//
//import java.io.File;
//import java.util.HashMap;
//
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import org.apache.spark.SparkContext;
//import org.apache.spark.sql.*;
//
//public class JavaXmlSuite {
//  private transient SQLContext sqlContext;
//  private int numCars = 3;
//
//  String carsFile = "src/test/resources/cars.xml";
//
//  private String tempDir = "target/test/xmlData/";
//
//  @Before
//  public void setUp() {
//    sqlContext = new SQLContext(new SparkContext("local[2]", "JavaXmlSuite"));
//  }
//
//  @After
//  public void tearDown() {
//    sqlContext.sparkContext().stop();
//    sqlContext = null;
//  }
//
//  @Test
//  public void testXmlParser() {
//    DataFrame df = (new XmlReader()).withUseHeader(true).xmlFile(sqlContext, carsFile);
//    int result = df.select("model").collect().length;
//    Assert.assertEquals(result, numCars);
//  }
//
//  @Test
//  public void testLoad() {
//    HashMap<String, String> options = new HashMap<String, String>();
//    options.put("header", "true");
//    options.put("path", carsFile);
//
//    DataFrame df = sqlContext.load("org.apache.spark.xml", options);
//    int result = df.select("year").collect().length;
//    Assert.assertEquals(result, numCars);
//  }
//
//  @Test
//  public void testSave() {
//    DataFrame df = (new XmlReader()).withUseHeader(true).xmlFile(sqlContext, carsFile);
//    TestUtils.deleteRecursively(new File(tempDir));
//    df.select("year", "model").save(tempDir, "org.apache.spark.xml");
//
//    DataFrame newDf = (new XmlReader()).xmlFile(sqlContext, tempDir);
//    int result = newDf.select("C1").collect().length;
//    Assert.assertEquals(result, numCars);
//
//  }
//}
