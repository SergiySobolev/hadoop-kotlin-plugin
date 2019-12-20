package com.sbk.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

class ConfigurationPrinter : Configured(), Tool {

    init {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-default.xml");
        Configuration.addDefaultResource("yarn-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    override fun run(p0: Array<out String>?): Int {
        conf.forEach {e: MutableMap.MutableEntry<String, String>? ->
            println("%s = %s".format(e?.key, e?.value))
        }
        return 0
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            ToolRunner.run(ConfigurationPrinter(), args)
        }

    }

}