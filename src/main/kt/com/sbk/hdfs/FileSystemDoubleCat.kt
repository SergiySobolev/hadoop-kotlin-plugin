package com.sbk.hdfs

import com.sbk.hdfs.Constants.BUFFER_SIZE
import com.sbk.hdfs.Constants.QUANGLE_FILE_URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI


class FileSystemDoubleCat(private var fsFileUri: String) {

    fun copyBytes() {
        val fs = FileSystem.get(URI.create(fsFileUri), Configuration())
        fs.open(Path(fsFileUri))
                .use { ins -> doubleCatStream(ins) }
    }

    private fun doubleCatStream(ins: FSDataInputStream) {
        ins.copyTo(System.out, BUFFER_SIZE)
        ins.seek(0)
        ins.copyTo(System.out, BUFFER_SIZE)
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            FileSystemDoubleCat(QUANGLE_FILE_URI).copyBytes()
        }
    }
}

