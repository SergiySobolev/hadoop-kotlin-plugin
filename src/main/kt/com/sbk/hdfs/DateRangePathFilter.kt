package com.sbk.hdfs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*
import java.util.regex.Pattern


class DateRangePathFilter(start: Date, end: Date) : PathFilter {

    private val PATTERN = Pattern.compile("^.*/(\\d\\d\\d\\d/\\d\\d/\\d\\d).*$")

    private val start: Date = Date(start.time)

    private val end: Date = Date(end.time)

    override fun accept(path: Path): Boolean {
        val matcher = PATTERN.matcher(path.toString())
        if (matcher.matches()) {
            val format: DateFormat = SimpleDateFormat("yyyy/MM/dd")
            return try {
                inInterval(format.parse(matcher.group(1)))
            } catch (e: ParseException) {
                false
            }
        }
        return false
    }

    private fun inInterval(date: Date): Boolean {
        return !date.before(start) && !date.after(end)
    }

}