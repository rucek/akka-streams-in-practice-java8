package org.kunicki.akka_streams_java8.importer;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import io.vavr.control.Try;
import org.kunicki.akka_streams_java8.model.InvalidReading;
import org.kunicki.akka_streams_java8.model.Reading;
import org.kunicki.akka_streams_java8.model.ValidReading;
import org.kunicki.akka_streams_java8.repository.ReadingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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

  private CompletionStage<Reading> parseLine(String line) {
    return CompletableFuture.supplyAsync(() -> {
      String[] fields = line.split(";");
      int id = Integer.parseInt(fields[0]);

      return Try
          .of(() -> Double.parseDouble(fields[1]))
          .map(value -> (Reading) new ValidReading(id, value))
          .onFailure(e -> logger.error("Unable to parse line: {}: {}", line, e.getMessage()))
          .getOrElse(new InvalidReading(id));
    });
  }
}
