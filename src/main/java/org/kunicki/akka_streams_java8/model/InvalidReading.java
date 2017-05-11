package org.kunicki.akka_streams_java8.model;

public class InvalidReading implements Reading {

  private final int id;

  public InvalidReading(int id) {
    this.id = id;
  }

  @Override
  public int getId() {
    return id;
  }
}
