package com.sbk.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import java.text.SimpleDateFormat
import java.util.*
import kotlin.collections.HashSet
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals


class FileSystemGlobTest {

    private val fs = FileSystem.get(Configuration())

    @BeforeTest
    fun setUp() {
        fs.mkdirs(Path(BASE_PATH, "2007/12/30"))
        fs.mkdirs(Path(BASE_PATH, "2007/12/31"))
        fs.mkdirs(Path(BASE_PATH, "2008/01/01"))
        fs.mkdirs(Path(BASE_PATH, "2008/01/02"))
    }

    @AfterTest
    fun tearDown() {
        fs.delete(Path(BASE_PATH), true)
    }

    @Test
    fun glob() {
        assertEquals(glob("/*"), paths("/2007", "/2008"))
        assertEquals(glob("/*/*"), paths("/2007/12", "/2008/01"))
        assertEquals(glob("/*/12/*"), paths("/2007/12/30", "/2007/12/31"))
        assertEquals(glob("/200?"), paths("/2007", "/2008"))
        assertEquals(glob("/200[78]"), paths("/2007", "/2008"))
        assertEquals(glob("/200[7-8]"), paths("/2007", "/2008"))
        assertEquals(glob("/200[^01234569]"), paths("/2007", "/2008"))
        assertEquals(glob("/*/*/{31,01}"), paths("/2007/12/31", "/2008/01/01"))
        assertEquals(glob("/*/*/3{0,1}"), paths("/2007/12/30", "/2007/12/31"))
        assertEquals(glob("/*/{12/31,01/01}"), paths("/2007/12/31", "/2008/01/01"))
    }

    @Test
    fun regexIncludes() {
        assertEquals(glob("/*", RegexPathFilter("^.*/2007$")), paths("/2007"))
        assertEquals(glob("/*/*/*", RegexPathFilter("^.*/2007/12/31$")), paths("/2007/12/31"))
        assertEquals(glob("/*/*/*", RegexPathFilter("^.*/2007(/12(/31)?)?$")), paths("/2007/12/31"))
    }

    @Test
    fun regexExcludes() {
        assertEquals(glob("/*", RegexPathFilter("^.*/2007$", false)), paths("/2008"))
        assertEquals(glob("/2007/*/*", RegexPathFilter("^.*/2007/12/31$", false)), paths("/2007/12/30"))
    }

    @Test
    fun regexExcludesWithRegexExcludePathFilter() {
        assertEquals(glob("/*", RegexExcludePathFilter("^.*/2007$")), paths("/2008"))
        assertEquals(glob("/2007/*/*", RegexExcludePathFilter("^.*/2007/12/31$")), paths("/2007/12/30"))
    }

    @Test
    fun testDateRange() {
        val filter = DateRangePathFilter(date("2007/12/31"), date("2008/01/01"))
        assertEquals(glob("/*/*/*", filter), paths("/2007/12/31", "/2008/01/01"))
    }

    private fun glob(pattern: String): Set<Path> {
        return HashSet(Arrays.asList(
                *FileUtil.stat2Paths(fs.globStatus(Path(BASE_PATH + pattern)))))
    }

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

    private fun date(date: String): Date {
        return SimpleDateFormat("yyyy/MM/dd").parse(date)
    }

    companion object {
        private val BASE_PATH = "/tmp/" + FileSystemGlobTest::class.java.simpleName
    }
}