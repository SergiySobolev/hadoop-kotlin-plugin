package com.sbk.hdfs

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import java.net.URL


class URLCat(var hdfsUrl:String) {

    fun copyBytes() {
        URL(hdfsUrl).openStream()
                .use { ins -> ins.copyTo(System.out, 4096) }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val hdfsUrl = "hdfs://localhost/user/hadoop/quangle.txt"
            URLCat(hdfsUrl).copyBytes()
        }
    }

    init {
        URL.setURLStreamHandlerFactory(FsUrlStreamHandlerFactory())
    }

}
