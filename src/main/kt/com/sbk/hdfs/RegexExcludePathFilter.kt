package com.sbk.hdfs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter


class RegexExcludePathFilter(private val regex: String) : PathFilter {
    override fun accept(path: Path): Boolean {
        return !Regex(regex).matches(path.toString())
    }

}