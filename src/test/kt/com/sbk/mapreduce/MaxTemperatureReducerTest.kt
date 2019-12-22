package com.sbk.mapreduce

import com.sbk.t3.MaxTemperatureReducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver
import kotlin.test.Test


class MaxTemperatureReducerTest {
    @Test
    fun returnsMaximumIntegerInValues() {
        ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(MaxTemperatureReducer())
                .withInput(Text("1950"), listOf(IntWritable(10), IntWritable(5)))
                .withOutput(Text("1950"), IntWritable(10))
                .runTest()
    }
}