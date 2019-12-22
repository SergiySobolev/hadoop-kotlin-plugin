package com.sbk.mapreduce.parser

import com.sbk.hdfs.Constants.MISSING_TEMPERATURE
import org.apache.hadoop.io.Text

class NcdcRecordParser constructor(val record: String) {

    constructor(text: Text) : this(text.toString())

    fun parse(): NcdcRecord {
        return if (record.length > 93) {
            val year = record.substring(15, 19)
            val airTemperatureString: String = if (record[87] == '+') {
                record.substring(88, 92)
            } else {
                record.substring(87, 92)
            }
            val airTemperature = airTemperatureString.toIntOrNull() ?: MISSING_TEMPERATURE
            val quality = record.substring(92, 93)
            NcdcRecord(year, airTemperature, quality)
        } else {
            NcdcRecord("-1", MISSING_TEMPERATURE, "NO_QUALITY")
        }

    }
}