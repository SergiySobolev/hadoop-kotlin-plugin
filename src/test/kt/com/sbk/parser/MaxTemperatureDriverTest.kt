package com.sbk.parser

import com.sbk.mapreduce.parser.MaxTemperatureDriver
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.junit.Test
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.stream.Collectors
import kotlin.test.assertEquals


class MaxTemperatureDriverTest {
    class OutputLogFilter : PathFilter {
        override fun accept(path: Path): Boolean {
            return !path.name.startsWith("_")
        }
    }

    @Test
    fun test() {
        val conf = Configuration()
        conf["fs.defaultFS"] = "file:///"
        conf["mapreduce.framework.name"] = "local"
        conf.setInt("mapreduce.task.io.sort.mb", 1)
        val input = Path("src/test/resources/input/ncdc/micro")
        val output = Path("output")
        val fs: FileSystem = FileSystem.getLocal(conf)
        fs.delete(output, true)
        val driver = MaxTemperatureDriver()
        driver.conf = conf
        val exitCode = driver.run(arrayOf(input.toString(), output.toString()))
        assertEquals(exitCode, 0)
        assertEquals(getOutputContent(conf, output), "1949\t111\n1950\t22")
    }

    private fun getOutputContent(conf: Configuration, output: Path): String {
        val fs: FileSystem = FileSystem.getLocal(conf)
        val outputFiles = FileUtil.stat2Paths(fs.listStatus(output, OutputLogFilter()))
        val actual = BufferedReader(InputStreamReader(fs.open(outputFiles[0])))
        return actual.lines().collect(Collectors.joining("\n"))
    }
}