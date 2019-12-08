package com.sbk.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.hadoop.fs.Path
import java.net.URI
import java.net.URL

class FileSystemCat(var fsFileUri:String) {

    fun copyBytes() {
        val fs = FileSystem.get(URI.create(fsFileUri), Configuration())
        fs.open(Path(fsFileUri))
                .use { ins -> ins.copyTo(System.out, 4096) }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val fsFileUri = "hdfs://localhost/user/hadoop/quangle.txt"
            FileSystemCat(fsFileUri).copyBytes()
        }
    }

    init {
        URL.setURLStreamHandlerFactory(FsUrlStreamHandlerFactory())
    }

}