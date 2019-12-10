package com.sbk.hdfs

import com.sbk.hdfs.Constants.USER_FOLDER_URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import java.net.URI


class ListStatus {

    fun show() {
        val fs = FileSystem.get(URI.create(USER_FOLDER_URI), Configuration())
        val paths = arrayOf(Path(USER_FOLDER_URI))
        val status = fs.listStatus(paths)
        val listedPaths = FileUtil.stat2Paths(status)
        listedPaths.forEach { listedPath -> println(listedPath) }
    }
}