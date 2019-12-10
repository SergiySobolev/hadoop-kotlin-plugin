package com.sbk.hdfs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter


class RegexPathFilter @JvmOverloads constructor(private val regex: String, private val include: Boolean = true) : PathFilter {
    override fun accept(path: Path): Boolean {
        return if (Regex(regex).matches(path.toString())) include else !include
    }

}