package com.sbk.mini

import com.sbk.mapreduce.parser.MaxTemperatureDriver
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.mapred.ClusterMapReduceTestCase
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull


class MaxTemperatureDriverMiniTest : ClusterMapReduceTestCase() {
    class OutputLogFilter : PathFilter {
        override fun accept(path: Path): Boolean {
            return !path.name.startsWith("_")
        }
    }

    @Throws(Exception::class)
    override fun setUp() {
        if (System.getProperty("test.build.data") == null) {
            System.setProperty("test.build.data", "/tmp")
        }
        if (System.getProperty("hadoop.log.dir") == null) {
            System.setProperty("hadoop.log.dir", "/tmp")
        }
        super.setUp()
    }

    @Test
    @Throws(Exception::class)
    fun test() {
        val conf: Configuration = createJobConf()
        val localInput = Path("src/test/resources/input/ncdc/micro")
        val input = inputDir
        val output = outputDir
        fileSystem.copyFromLocalFile(localInput, input)
        val driver = MaxTemperatureDriver()
        driver.conf = conf
        val exitCode = driver.run(arrayOf(input.toString(), output.toString()))
        assertEquals(exitCode, 0)
        val outputFiles = FileUtil.stat2Paths(fileSystem.listStatus(output, OutputLogFilter()))
        assertEquals(outputFiles.size, 1)
        val ins: InputStream = fileSystem.open(outputFiles[0])
        val reader = BufferedReader(InputStreamReader(ins))
        assertEquals(reader.readLine(), "1949\t111")
        assertEquals(reader.readLine(), "1950\t22")
        assertNull(reader.readLine())
        reader.close()
    }
}