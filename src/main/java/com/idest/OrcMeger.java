package com.idest;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;

import java.io.File;
import java.io.IOException;

public class OrcMeger {
	public static void main(String[] args) throws IOException {
		Options options = getOptions();

		// コマンドライン解析
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("cmd parser failed.: " + e);
			System.exit(-1);
		}

		// 引数のエラーチェック
		if (cmd.getArgs().length < 1) {
			// オプションのヘルプ情報を表示する。
			HelpFormatter hf = new HelpFormatter();
			hf.printHelp("[opts] <merge orc directory>", options);
			System.exit(-1);
		}

		String outDir = "./";
		if (cmd.hasOption("o")) {
			outDir = cmd.getOptionValue("o");
		}
		System.out.println("OUT DIR: " + outDir);

		exec(cmd.getArgs()[0]);
	}

	private static Options getOptions() {
		// コマンドラインオプションの設定
		Options options = new Options();

		// 出力ディレクトリ
		Option outDirOpt  = Option.builder("o")
				.argName("out_dir")
				.hasArg()
				.desc("output directory").build();
		options.addOption(outDirOpt);

		return options;
	}

	private static void exec(String dirName) throws IOException {
		File dir = new File(dirName);
		File[] list = dir.listFiles();

		Writer writer = null;
		for(File file : list) {
			if(!file.isFile()) continue;

			try {
				System.out.println("read: " + file.getAbsolutePath()); // ファイル名のみ

				Configuration conf = new Configuration();
				Reader reader = OrcFile.createReader(new Path(file.getAbsolutePath()), OrcFile.readerOptions(conf));
				// StructObjectInspector inspector = (StructObjectInspector)reader.getObjectInspector();

				if (writer == null) { // writerの初期化
					OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
							.compress(reader.getCompression())
							.inspector(reader.getObjectInspector());
					writer = OrcFile.createWriter(new Path("./00000_0"), writerOptions);
				}

				RecordReader records = reader.rows();
				Object row = null;
				int count = 0;
				while(records.hasNext())
				{
					count++;
					row = records.next(row);
					writer.addRow(row);
				}
				System.out.println("READ COUNT: " + count);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (writer != null) {
			writer.close();
		}
	}
}
