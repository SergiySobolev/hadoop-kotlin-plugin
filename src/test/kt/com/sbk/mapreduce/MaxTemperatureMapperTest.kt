package com.sbk.mapreduce

import com.sbk.t3.MaxTemperatureMapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mrunit.mapreduce.MapDriver
import java.io.IOException
import kotlin.test.Test


class MaxTemperatureMapperTest {

    @Test
    fun processesValidRecord(): Unit {
        val record = "0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999"
        val value = Text(record)
        MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(MaxTemperatureMapper())
                .withInput(LongWritable(0), value)
                .withOutput(Text("1950"), IntWritable(-11))
                .runTest()
    }

    @Test
    @Throws(IOException::class, InterruptedException::class)
    fun processesPositiveTemperatureRecord() {
        val value = Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9+00111+99999999999")
        MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(MaxTemperatureMapper())
                .withInput(LongWritable(0), value)
                .withOutput(Text("1950"), IntWritable(11))
                .runTest()
    }

    @Test
    @Throws(IOException::class, InterruptedException::class)
    fun ignoresMissingTemperatureRecord() {
        val value = Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9+99991+99999999999")
        MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(MaxTemperatureMapper())
                .withInput(LongWritable(0), value)
                .runTest()
    }

    @Test
    @Throws(IOException::class, InterruptedException::class)
    fun ignoresSuspectQualityRecord() {
        val value = Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9+00112+99999999999")
        MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(MaxTemperatureMapper())
                .withInput(LongWritable(0), value)
                .runTest()
    }
}