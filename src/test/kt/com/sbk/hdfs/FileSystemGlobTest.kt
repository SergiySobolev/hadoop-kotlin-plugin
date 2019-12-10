package com.sbk.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.io.IOException
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*
import kotlin.collections.HashSet


class FileSystemGlobTest {
    private val fs = FileSystem.get(Configuration())
    @Before
    @Throws(Exception::class)
    fun setUp() {
        fs.mkdirs(Path(BASE_PATH, "2007/12/30"))
        fs.mkdirs(Path(BASE_PATH, "2007/12/31"))
        fs.mkdirs(Path(BASE_PATH, "2008/01/01"))
        fs.mkdirs(Path(BASE_PATH, "2008/01/02"))
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        fs?.delete(Path(BASE_PATH), true)
    }

    @Test
    @Throws(Exception::class)
    fun glob() {
        Assert.assertEquals(glob("/*"), paths("/2007", "/2008"))
        Assert.assertEquals(glob("/*/*"), paths("/2007/12", "/2008/01"))
        Assert.assertEquals(glob("/*/12/*"), paths("/2007/12/30", "/2007/12/31"))
        Assert.assertEquals(glob("/200?"), paths("/2007", "/2008"))
        Assert.assertEquals(glob("/200[78]"), paths("/2007", "/2008"))
        Assert.assertEquals(glob("/200[7-8]"), paths("/2007", "/2008"))
        Assert.assertEquals(glob("/200[^01234569]"), paths("/2007", "/2008"))
        Assert.assertEquals(glob("/*/*/{31,01}"), paths("/2007/12/31", "/2008/01/01"))
        Assert.assertEquals(glob("/*/*/3{0,1}"), paths("/2007/12/30", "/2007/12/31"))
        Assert.assertEquals(glob("/*/{12/31,01/01}"), paths("/2007/12/31", "/2008/01/01"))
    }

    @Test
    @Throws(Exception::class)
    fun regexIncludes() {
        Assert.assertEquals(glob("/*", RegexPathFilter("^.*/2007$")), paths("/2007"))
        Assert.assertEquals(glob("/*/*/*", RegexPathFilter("^.*/2007/12/31$")), paths("/2007/12/31"))
        Assert.assertEquals(glob("/*/*/*", RegexPathFilter("^.*/2007(/12(/31)?)?$")), paths("/2007/12/31"))
    }

    @Test
    @Throws(Exception::class)
    fun regexExcludes() {
        Assert.assertEquals(glob("/*", RegexPathFilter("^.*/2007$", false)), paths("/2008"))
        Assert.assertEquals(glob("/2007/*/*", RegexPathFilter("^.*/2007/12/31$", false)), paths("/2007/12/30"))
    }

    @Test
    @Throws(Exception::class)
    fun regexExcludesWithRegexExcludePathFilter() {
        Assert.assertEquals(glob("/*", RegexExcludePathFilter("^.*/2007$")), paths("/2008"))
        Assert.assertEquals(glob("/2007/*/*", RegexExcludePathFilter("^.*/2007/12/31$")), paths("/2007/12/30"))
    }

    @Test
    @Throws(Exception::class)
    fun testDateRange() {
        val filter = DateRangePathFilter(date("2007/12/31"),
                date("2008/01/01"))
        Assert.assertEquals(glob("/*/*/*", filter), paths("/2007/12/31", "/2008/01/01"))
    }

    @Throws(IOException::class)
    private fun glob(pattern: String): Set<Path> {
        return HashSet(Arrays.asList(
                *FileUtil.stat2Paths(fs!!.globStatus(Path(BASE_PATH + pattern)))))
    }

    @Throws(IOException::class)
    private fun glob(pattern: String, pathFilter: PathFilter): Set<Path> {
        return HashSet(Arrays.asList(
                *FileUtil.stat2Paths(fs!!.globStatus(Path(BASE_PATH + pattern), pathFilter))))
    }

    private fun paths(vararg pathStrings: String): Set<Path> {
        val paths = arrayOfNulls<Path>(pathStrings.size)
        for (i in paths.indices) {
            paths[i] = Path("file:" + BASE_PATH + pathStrings[i])
        }
        return HashSet<Path>(Arrays.asList(*paths))
    }

    @Throws(ParseException::class)
    private fun date(date: String): Date {
        return SimpleDateFormat("yyyy/MM/dd").parse(date)
    }

    companion object {
        private val BASE_PATH = "/tmp/" +
                FileSystemGlobTest::class.java.simpleName
    }
}