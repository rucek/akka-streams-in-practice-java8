package org.kunicki.akka_streams_java8.importer;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.typesafe.config.Config;
import io.vavr.collection.List;
import io.vavr.control.Try;
import org.kunicki.akka_streams_java8.model.InvalidReading;
import org.kunicki.akka_streams_java8.model.Reading;
import org.kunicki.akka_streams_java8.model.ValidReading;
import org.kunicki.akka_streams_java8.repository.ReadingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.zip.GZIPInputStream;

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

  private Flow<ByteString, ByteString, NotUsed> lineDelimiter =
      Framing.delimiter(ByteString.fromString("\n"), 128, FramingTruncation.ALLOW);

  private Flow<File, Reading, NotUsed> parseFile() {
    return Flow.of(File.class).flatMapConcat(file -> {
      GZIPInputStream inputStream = new GZIPInputStream(new FileInputStream(file));
      return StreamConverters.fromInputStream(() -> inputStream)
          .via(lineDelimiter)
          .drop(linesToSkip)
          .map(ByteString::utf8String)
          .mapAsync(nonIOParallelism, this::parseLine);
    });
  }

  private Flow<Reading, ValidReading, NotUsed> computeAverage() {
    return Flow.of(Reading.class).grouped(2).mapAsyncUnordered(nonIOParallelism, readings ->
        CompletableFuture.supplyAsync(() -> {
          List<ValidReading> validReadings = List.ofAll(readings)
              .filter(ValidReading.class::isInstance)
              .map(ValidReading.class::cast);

          double average = validReadings.map(ValidReading::getValue).average().getOrElse(-1.0);

          return new ValidReading(readings.get(0).getId(), average);
        }));
  }

  private Sink<ValidReading, CompletionStage<Done>> storeReadings() {
    return Flow.of(ValidReading.class)
        .mapAsyncUnordered(concurrentWrites, readingRepository::save)
        .toMat(Sink.ignore(), Keep.right());
  }

  private Flow<File, ValidReading, NotUsed> processSingleFile() {
    return Flow.of(File.class)
        .via(parseFile())
        .via(computeAverage());
  }
}
