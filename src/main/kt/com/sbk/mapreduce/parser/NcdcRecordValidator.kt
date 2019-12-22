package com.sbk.mapreduce.parser

import com.sbk.hdfs.Constants
import org.slf4j.LoggerFactory

class NcdcRecordValidator(private val record: NcdcRecord?) {

    private val logger = LoggerFactory.getLogger(NcdcRecordValidator::class.java)

    fun isValid(): Boolean {
//        val res = (record?.airTemperature == Constants.MISSING_TEMPERATURE
//                && record.quality.matches(Regex.fromLiteral("[01459]")))
        val res = record?.airTemperature != Constants.MISSING_TEMPERATURE
        if (!res) {
            logger.error("Record {} is invalid", record)
        }
        return res
    }
}