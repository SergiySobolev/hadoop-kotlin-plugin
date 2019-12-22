@file:JvmName("Launcher")

package com.sbk.mapreduce.parser

import com.sbk.t3.MaxTemperatureReducer
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import kotlin.system.exitProcess


class MaxTemperatureDriver : Configured(), Tool {

    override fun run(args: Array<String>): Int {
        if (args.size != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", javaClass.simpleName)
            ToolRunner.printGenericCommandUsage(System.err)
            return -1
        }
        val job = Job.getInstance(conf, "Max temperature")
        job.setJarByClass(javaClass)
        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, Path(args[1]))
        job.mapperClass = MaxTemperatureMapperV2::class.java
        job.combinerClass = MaxTemperatureReducer::class.java
        job.reducerClass = MaxTemperatureReducer::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java
        return if (job.waitForCompletion(true)) 0 else 1
    }

    companion object {
        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val exitCode = ToolRunner.run(MaxTemperatureDriver(), args)
            exitProcess(exitCode)
        }
    }
}