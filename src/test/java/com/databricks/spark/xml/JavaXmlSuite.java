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
import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import com.databricks.spark.xml.util.XmlFile;

public class JavaXmlSuite {
    private transient SQLContext sqlContext;
    private int numBooks = 12;

    String booksFile = "src/test/resources/books.xml";
    String booksFileTag = "book";

    private String tempDir = "target/test/xmlData/";

    @Before
    public void setUp() {
        sqlContext = new SQLContext(new SparkContext("local[2]", "JavaXmlSuite"));
    }

    @After
    public void tearDown() {
        sqlContext.sparkContext().stop();
        sqlContext = null;
    }

    @Test
    public void testXmlParser() {
        DataFrame df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(sqlContext, booksFile);
        String prefix = XmlFile.DEFAULT_ATTRIBUTE_PREFIX();
        int result = df.select(prefix + "id").collect().length;
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testLoad() {
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("rowTag", booksFileTag);
        options.put("path", booksFile);

        DataFrame df = sqlContext.load("com.databricks.spark.xml", options);
        int result = df.select("description").collect().length;
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testSave() {
        DataFrame df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(sqlContext, booksFile);
        TestUtils.deleteRecursively(new File(tempDir));
        df.select("price", "description").save(tempDir, "com.databricks.spark.xml");

        DataFrame newDf = (new XmlReader()).xmlFile(sqlContext, tempDir);
        int result = newDf.select("price").collect().length;
        Assert.assertEquals(result, numBooks);
    }
}
