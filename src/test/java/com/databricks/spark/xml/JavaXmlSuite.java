/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.xml;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

public class JavaXmlSuite {
    private static final int numBooks = 12;
    private static final String booksFile = "src/test/resources/books.xml";
    private static final String booksFileTag = "book";
    private static final String tempDir = "target/test/xmlData/";

    private transient SQLContext sqlContext;

    @Before
    public void setUp() throws IOException {
        // Fix Spark 2.0.0 on windows, see https://issues.apache.org/jira/browse/SPARK-15893
        SparkConf conf = new SparkConf().set(
                "spark.sql.warehouse.dir",
                Files.createTempDirectory("spark-warehouse").toString());
        sqlContext = new SQLContext(new SparkContext("local[2]", "JavaXmlSuite", conf));
    }

    @After
    public void tearDown() {
        sqlContext.sparkContext().stop();
        sqlContext = null;
    }

    @Test
    public void testXmlParser() {
        Dataset df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(sqlContext, booksFile);
        String prefix = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX();
        long result = df.select(prefix + "id").count();
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testLoad() {
        HashMap<String, String> options = new HashMap<>();
        options.put("rowTag", booksFileTag);
        options.put("path", booksFile);

        Dataset df = sqlContext.load("com.databricks.spark.xml", options);
        long result = df.select("description").count();
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testSave() {
        Dataset df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(sqlContext, booksFile);
        TestUtils.deleteRecursively(new File(tempDir));
        df.select("price", "description").write().format("xml").save(tempDir);

        Dataset newDf = (new XmlReader()).xmlFile(sqlContext, tempDir);
        long result = newDf.select("price").count();
        Assert.assertEquals(result, numBooks);
    }
}
