package com.sbk.t3

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class MaxTemperatureMapper : Mapper<LongWritable, Text, Text, IntWritable>() {

    private val MISSING = 9999

    override fun map(key: LongWritable, value: Text, context: Context) {
        val line = value.toString()
        val year = line.substring(15, 19)
        val airTemperature: Int
        airTemperature = if (line[87] == '+') {
            line.substring(88, 92).toInt()
        } else {
            line.substring(87, 92).toInt()
        }
        val quality = line.substring(92, 93)
        if (airTemperature != MISSING && quality.matches(Regex("[01459]"))) {
            context.write(Text(year), IntWritable(airTemperature))
        }
    }
}

