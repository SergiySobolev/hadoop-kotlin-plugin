package com.sbk.t3

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import java.io.IOException

class MaxTemperatureReducer : Reducer<Text?, IntWritable, Text?, IntWritable?>() {
    @Throws(IOException::class, InterruptedException::class)
    public override fun reduce(
            key: Text?,
            values: Iterable<IntWritable>,
            context: Context) {
        var maxValue = Int.MIN_VALUE
        for (value in values) {
            maxValue = Math.max(maxValue, value.get())
        }
        context.write(key, IntWritable(maxValue))
    }
}