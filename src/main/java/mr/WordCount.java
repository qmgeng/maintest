package mr; /**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static void main(String[] args) throws Exception {
        // kerberos的配置文件的位置,windows下叫krb5.ini,linux下叫krb5.conf
        System.setProperty("java.security.krb5.conf", "C:\\Windows\\krb5.ini");

        // win下的配置，防止出现winutil.exe的异常，hadoop.home.dir\bin目录下要有编译好的exe
        // 不配这个属性也没影响，就是会有个异常，不影响运行
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2");

        Configuration conf = new Configuration();
        // 使用keytab登陆
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("weblog/dev@HADOOP.HZ.NETEASE.COM", "E:\\163\\weblog.keytab");

        // String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // if (otherArgs.length < 2) {
        // System.err.println("Usage: wordcount <in> [<in>...] <out>");
        // System.exit(2);
        // }
        Job job = new Job(conf, "word-count-test");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // for (int i = 0; i < otherArgs.length - 1; ++i) {
        // FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        // }
        // FileOutputFormat.setOutputPath(job,
        // new Path(otherArgs[otherArgs.length - 1]));
        FileInputFormat
                .addInputPath(
                        job,
                        new Path(
                                "/ntes_weblog/hot/hotLog/hot_bh/20150629/163wssa_nethot_operate_20150629-235158592+0800.65059169437913071.01348251.lzo"));
        FileOutputFormat.setOutputPath(job, new Path("/ntes_weblog/hot/temp/0630test"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
