/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.xml;

import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

public class JavaXmlSuite {
    private transient SQLContext sqlContext;
    private int numCars = 12;

    String booksFile = "src/test/resources/books.xml";
    String booksFileTag = "book";

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
        DataFrame df = (new XmlReader()).withRootTag(booksFileTag).xmlFile(sqlContext, booksFile);
        int result = df.select("id").collect().length;
        Assert.assertEquals(result, numCars);
    }

    @Test
    public void testLoad() {
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("option", booksFileTag);
        options.put("path", booksFile);

        DataFrame df = sqlContext.load("org.apache.spark.xml", options);
        int result = df.select("description").collect().length;
        Assert.assertEquals(result, numCars);
    }
}
