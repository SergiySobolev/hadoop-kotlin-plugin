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
            if(record.airTemperature > 900) {
                System.err.println("Temperature = {} over 90 degrees for input {}".format(record.airTemperature, value))
                context.status = "Detected possibly corrupted record: see logs"
                context.getCounter(Temperature.OVER_90).increment(1)
            }
            context.write(Text(record.year), IntWritable(record.airTemperature))
        }
    }
}

enum class Temperature {
    OVER_90
}