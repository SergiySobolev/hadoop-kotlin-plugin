package com.sbk.hdfs

import com.sbk.hdfs.Constants.BIG_TEXT_FILE
import com.sbk.hdfs.Constants.BUFFER_SIZE
import com.sbk.hdfs.Constants.USER_FOLDER_URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Progressable
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.URI


class FileCopyWithProgress {
    fun copyTo() {
        val localSrc = BIG_TEXT_FILE
        val dst = USER_FOLDER_URI + "bigtextfile.txt"
        val ins: InputStream = BufferedInputStream(FileInputStream(localSrc))
        val conf = Configuration()
        val fs = FileSystem.get(URI.create(dst), conf)
        val p = Progressable { print(".") }
        val out: OutputStream = fs.create(Path(dst), p)
        ins.use { s ->
            s.copyTo(out, BUFFER_SIZE)
        }
    }
}