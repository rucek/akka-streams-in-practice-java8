package org.kunicki.akka_streams_java8.repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.kunicki.akka_streams_java8.model.ValidReading;

import java.util.concurrent.CompletionStage;

import static net.javacrumbs.futureconverter.java8guava.FutureConverter.toCompletableFuture;

public class ReadingRepository {

  private final Session session = Cluster.builder().addContactPoint("127.0.0.1").build().connect();

  private final PreparedStatement preparedStatement = session.prepare("insert into akka_streams.readings (id, value) values (?, ?)");

  public CompletionStage<ResultSet> save(ValidReading validReading) {
    BoundStatement boundStatement = preparedStatement.bind(validReading.getId(), (float) validReading.getValue());
    return toCompletableFuture(session.executeAsync(boundStatement));
  }

  public void shutdown() {
    session.getCluster().close();
  }
}
