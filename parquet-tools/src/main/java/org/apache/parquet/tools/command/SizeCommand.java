/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.tools.command;

import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.tools.Main;

public class SizeCommand extends ArgsOnlyCommand {
    private FileStatus inputFileStatus;
    private Configuration conf;
    private Path inputPath;
    private PrintWriter out;
    public static final String[] USAGE = new String[] {
            "<input>",
            "where <input> is the parquet file to get size & human readable size to stdout"
    };

    public static final Options OPTIONS;
    static {
        OPTIONS = new Options();
        Option help = OptionBuilder.withLongOpt("pretty")
                .withDescription("Pretty size")
                .create('p');
        OPTIONS.addOption(help);
    }

    public SizeCommand() {
        super(1, 1);
    }

    @Override
    public Options getOptions() {
        return OPTIONS;
    }

    @Override
    public String[] getUsageDescription() {
        return USAGE;
    }

    @Override
    public void execute(CommandLine options) throws Exception {
        super.execute(options);

        String[] args = options.getArgs();
        String input = args[0];
        inputPath = new Path(input);
        conf = new Configuration();
        inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath);
        long size = 0;
        List<Footer> footers = ParquetFileReader.readFooters(conf, inputFileStatus, false);
        for(Footer f : footers){
            for(BlockMetaData b : f.getParquetMetadata().getBlocks()){
                size += b.getCompressedSize();
            }
        }
        out = new PrintWriter(Main.out, true);
        if(options.hasOption('p')){
            out.format("Size: %s", getPrettySize(size));
        }
        else{
            out.format("Size: %d bytes", size);
        }
        out.println();
    }

    public String getPrettySize(long bytes){
        double oneKB = 1024;
        double oneMB = oneKB * 1024;
        double oneGB = oneMB * 1014;
        double oneTB = oneGB * 1024;
        double onePB = oneTB * 1024;
        if (bytes/oneKB < 1){
            return  String.format("%.3f", bytes) + " bytes";
        }
        if (bytes/oneMB < 1){
            return String.format("%.3f", bytes/oneKB) + " KB";
        }
        if (bytes/oneGB < 1){
            return String.format("%.3f", bytes/oneMB) + " MB";
        }
        if (bytes/oneTB < 1){
            return String.format("%.3f", bytes/oneGB) + " GB";
        }
        return String.valueOf(bytes/onePB) + " PB";
    }
}