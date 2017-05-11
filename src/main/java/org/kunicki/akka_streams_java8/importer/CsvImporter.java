package org.kunicki.akka_streams_java8.importer;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.kunicki.akka_streams_java8.repository.ReadingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;

public class CsvImporter {

  private static final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

  private final ReadingRepository readingRepository;
  private final ActorSystem system;

  private final File importDirectory;
  private final int linesToSkip;
  private final int concurrentFiles;
  private final int concurrentWrites;
  private final int nonIOParallelism;

  private CsvImporter(Config config, ReadingRepository readingRepository, ActorSystem system) {
    this.readingRepository = readingRepository;
    this.system = system;
    this.importDirectory = Paths.get(config.getString("importer.import-directory")).toFile();
    this.linesToSkip = config.getInt("importer.lines-to-skip");
    this.concurrentFiles = config.getInt("importer.concurrent-files");
    this.concurrentWrites = config.getInt("importer.concurrent-writes");
    this.nonIOParallelism = config.getInt("importer.non-io-parallelism");
  }
}
