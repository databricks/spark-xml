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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public final class JavaXmlSuite {

    private static final int numBooks = 12;
    private static final String booksFile = "src/test/resources/books.xml";
    private static final String booksFileTag = "book";

    private SparkSession spark;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        spark = SparkSession.builder().
            master("local[2]").
            appName("XmlSuite").
            config("spark.ui.enabled", false).
            getOrCreate();
        tempDir = Files.createTempDirectory("JavaXmlSuite");
        tempDir.toFile().deleteOnExit();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    private Path getEmptyTempDir() throws IOException {
        return Files.createTempDirectory(tempDir, "test");
    }

    @Test
    public void testXmlParser() {
        Dataset df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(spark, booksFile);
        String prefix = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX();
        long result = df.select(prefix + "id").count();
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testLoad() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", booksFileTag);
        Dataset df = spark.read().options(options).format("xml").load(booksFile);
        long result = df.select("description").count();
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testSave() throws IOException {
        Path booksPath = getEmptyTempDir().resolve("booksFile");

        Dataset df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(spark, booksFile);
        df.select("price", "description").write().format("xml").save(booksPath.toString());

        Dataset newDf = (new XmlReader()).xmlFile(spark, booksPath.toString());
        long result = newDf.select("price").count();
        Assert.assertEquals(result, numBooks);
    }

}
