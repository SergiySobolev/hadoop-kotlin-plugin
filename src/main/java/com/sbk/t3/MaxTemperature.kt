package com.sbk.t3

import com.sbk.t2.MaxTemperatureMapper
import com.sbk.t2.MaxTemperatureReducer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


object MaxTemperature {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>")
            System.exit(-1)
        }
        val job = Job.getInstance()
        job.setJarByClass(MaxTemperature::class.java)
        job.jobName = "Max temperature"
        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, Path(args[1]))
        job.mapperClass = MaxTemperatureMapper::class.java
        job.reducerClass = MaxTemperatureReducer::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java
        System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }
}