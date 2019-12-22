package com.sbk.mapreduce.parser

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory

class MaxTemperatureMapperV2 : Mapper<LongWritable?, Text?, Text?, IntWritable?>() {

    private val logger = LoggerFactory.getLogger(MaxTemperatureMapperV2::class.java)

    public override fun map(key: LongWritable?, value: Text?, context: Context) {
        val record = value?.let { NcdcRecordParser(it).parse() }
        if (NcdcRecordValidator(record).isValid()) {
            logger.info("Writing to context year = [{}], airTemperature=[{}]", record!!.year, record.airTemperature)
            context.write(Text(record.year), IntWritable(record.airTemperature))
        }
    }
}