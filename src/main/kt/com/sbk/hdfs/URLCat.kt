package com.sbk.hdfs

import com.sbk.hdfs.Constants.BUFFER_SIZE
import com.sbk.hdfs.Constants.QUANGLE_FILE_URI
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import java.net.URL


class URLCat(var hdfsUrl:String) {

    fun copyBytes() {
        URL(hdfsUrl).openStream()
                .use { ins -> ins.copyTo(System.out, BUFFER_SIZE) }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            URLCat(QUANGLE_FILE_URI).copyBytes()
        }
    }

    init {
        URL.setURLStreamHandlerFactory(FsUrlStreamHandlerFactory())
    }

}
