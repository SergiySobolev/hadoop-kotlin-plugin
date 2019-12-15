package com.sbk.sequencefile

import com.sbk.hdfs.Constants.DFS_URI
import com.sbk.hdfs.Constants.USER_FOLDER_URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.Text
import java.net.URI

class SequenceFileWriteDemo(private var filename: String) {

    private val DATA = arrayOf(
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    )

    fun createSequenceFile() {
        val uri = USER_FOLDER_URI + filename
        val conf = Configuration()
        val fs = FileSystem.get(URI.create(DFS_URI), conf)
        val path = Path(uri)

        val writer = SequenceFile.createWriter(
                conf,
                Writer.stream(fs.create(path)),
                Writer.keyClass(IntWritable::class.java),
                Writer.valueClass(Text::class.java)
        )
        writer.use { w -> writeDataToFile(w) }
    }

    private fun writeDataToFile(w: Writer) {
        val key = IntWritable()
        val value = Text()
        for (i in 0..99) {
            key.set(100 - i)
            value.set(DATA[i % DATA.size])
            println("[%s]\t%s\t%s".format(w.length, key, value))
            w.append(key, value)
        }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SequenceFileWriteDemo("numbers.seq").createSequenceFile()
        }
    }
}