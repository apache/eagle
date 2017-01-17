/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.analyzer.mr.suggestion;

import org.apache.eagle.jpm.analyzer.Processor;
import org.apache.eagle.jpm.analyzer.meta.model.MapReduceAnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static org.apache.hadoop.mapreduce.MRJobConfig.MAP_OUTPUT_COMPRESS;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.MRJobConfig.NUM_REDUCES;
import static org.apache.hadoop.mapreduce.MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR;

public class MapReduceCompressionSettingAnalyzer implements Processor<MapReduceAnalyzerEntity> {

    private MapReduceJobSuggestionContext context;

    public MapReduceCompressionSettingAnalyzer(MapReduceJobSuggestionContext context) {
        this.context = context;
    }

    @Override
    public Result.ProcessorResult process(MapReduceAnalyzerEntity jobAnalysisEntity) {
        StringBuilder sb = new StringBuilder();
        JobConf jobconf = new JobConf(context.getJobconf());
        if (jobconf.getLong(NUM_REDUCES, 0) > 0) {
            if (jobconf.getCompressMapOutput() == false) {
                sb.append("Please set " + MAP_OUTPUT_COMPRESS
                    + " to true to reduce network IO.\n");
            } else {
                String codecClassName = jobconf
                    .get(MAP_OUTPUT_COMPRESS_CODEC);
                if (!(codecClassName.endsWith("LzoCodec") || codecClassName
                    .endsWith("SnappyCodec"))) {
                    sb.append("Best practice: use LzoCodec or SnappyCodec for "
                        + MAP_OUTPUT_COMPRESS_CODEC);
                    sb.append("\n");
                }
            }
        }

        if (!jobconf.getBoolean(FileOutputFormat.COMPRESS, false)) {
            sb.append("Please set " + FileOutputFormat.COMPRESS
                + " to true to reduce disk usage and network IO.\n");
        } else {
            String codecName = jobconf.get(FileOutputFormat.COMPRESS_CODEC, "");
            String outputFileFormat = jobconf.get(OUTPUT_FORMAT_CLASS_ATTR, "");

            if ((codecName.endsWith("GzipCodec") ||
                codecName.endsWith("SnappyCodec") ||
                codecName.endsWith("DefaultCodec"))
                && outputFileFormat.endsWith("TextOutputFormat")) {
                sb.append("Best practice: don't use Gzip/Snappy/DefaultCodec with TextOutputFormat");
                sb.append(" as this will cause the output files to be unsplittable. ");
                sb.append("Please use LZO instead or ");
                sb.append("use a container file format such as SequenceFileOutputFormat.\n");
            }
        }

        if (sb.length() > 0) {
            return new Result.ProcessorResult(Result.ResultLevel.WARNING, sb.toString());
        }
        return null;
    }
}
