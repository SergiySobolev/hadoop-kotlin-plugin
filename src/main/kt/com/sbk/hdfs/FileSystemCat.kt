package com.sbk.hdfs

import com.sbk.hdfs.Constants.BUFFER_SIZE
import com.sbk.hdfs.Constants.QUANGLE_FILE_URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI

class FileSystemCat(private var fsFileUri: String) {

    fun copyBytes() {
        val fs = FileSystem.get(URI.create(fsFileUri), Configuration())
        fs.open(Path(fsFileUri))
                .use { ins -> ins.copyTo(System.out, BUFFER_SIZE) }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            FileSystemCat(QUANGLE_FILE_URI).copyBytes()
        }
    }

}