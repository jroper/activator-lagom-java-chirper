/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.chirper.friend.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.lightbend.lagom.javadsl.persistence.AggregateEventTag;
import com.lightbend.lagom.javadsl.persistence.cassandra.CassandraReadSideProcessor;
import com.lightbend.lagom.javadsl.persistence.cassandra.CassandraSession;

import akka.Done;
import sample.chirper.friend.impl.FriendEvent.*;

public class FriendEventProcessor extends CassandraReadSideProcessor<FriendEvent> {

  private PreparedStatement addRequester = null; // initialized in prepare
  private PreparedStatement deleteRequester = null; // initialized in prepare
  private PreparedStatement writeFollowers = null; // initialized in prepare
  private PreparedStatement writeOffset = null; // initialized in prepare

  private void setWriteFollowers(PreparedStatement writeFollowers) {
    this.writeFollowers = writeFollowers;
  }

  private void setAddRequester(PreparedStatement addRequester) {
    this.addRequester = addRequester;
  }

  private void setDeleteRequester(PreparedStatement deleteRequester) {
    this.deleteRequester = deleteRequester;
  }

  private void setWriteOffset(PreparedStatement writeOffset) {
    this.writeOffset = writeOffset;
  }

  @Override
  public AggregateEventTag<FriendEvent> aggregateTag() {
    return FriendEventTag.INSTANCE;
  }

  @Override
  public CompletionStage<Optional<UUID>> prepare(CassandraSession session) {
    // @formatter:off
    return
      prepareCreateTables(session).thenCompose(a ->
      prepareWriteFollowers(session)).thenCompose(a ->
      prepareAddRequester(session)).thenCompose(a ->
      prepareDeleteRequester(session)).thenCompose(a ->
      prepareWriteOffset(session)).thenCompose(a ->
      selectOffset(session));
    // @formatter:on
  }

  private CompletionStage<Done> prepareCreateTables(CassandraSession session) {
    // @formatter:off
    return session.executeCreateTable(
        "CREATE TABLE IF NOT EXISTS follower ("
          + "userId text, followedBy text, "
          + "PRIMARY KEY (userId, followedBy))")
      .thenCompose(a -> session.executeCreateTable(
        "CREATE TABLE IF NOT EXISTS requester ("
          + "userId text, requestedBy text, "
          + "PRIMARY KEY (userId, requestedBy))"))
      .thenCompose(a -> session.executeCreateTable(
        "CREATE TABLE IF NOT EXISTS friend_offset ("
          + "partition int, offset timeuuid, "
          + "PRIMARY KEY (partition))"));
    // @formatter:on
  }

  private CompletionStage<Done> prepareWriteFollowers(CassandraSession session) {
    return session.prepare("INSERT INTO follower (userId, followedBy) VALUES (?, ?)").thenApply(ps -> {
      setWriteFollowers(ps);
      return Done.getInstance();
    });
  }

  private CompletionStage<Done> prepareAddRequester(CassandraSession session) {
    return session.prepare("INSERT INTO requester (userId, requestedBy) VALUES (?, ?)").thenApply(ps -> {
      setAddRequester(ps);
      return Done.getInstance();
    });
  }

  private CompletionStage<Done> prepareDeleteRequester(CassandraSession session) {
    return session.prepare("DELETE FROM requester WHERE userId = ? AND requestedBy = ?").thenApply(ps -> {
      setDeleteRequester(ps);
      return Done.getInstance();
    });
  }

  private CompletionStage<Done> prepareWriteOffset(CassandraSession session) {
    return session.prepare("INSERT INTO friend_offset (partition, offset) VALUES (1, ?)").thenApply(ps -> {
      setWriteOffset(ps);
      return Done.getInstance();
    });
  }

  private CompletionStage<Optional<UUID>> selectOffset(CassandraSession session) {
    return session.selectOne("SELECT offset FROM friend_offset")
        .thenApply(
        optionalRow -> optionalRow.map(r -> r.getUUID("offset")));
  }

  @Override
  public EventHandlers defineEventHandlers(EventHandlersBuilder builder) {
    builder.setEventHandler(FriendAccepted.class, this::processFriendChanged);
    builder.setEventHandler(FriendRequested.class, this::processRequestAdded);
    builder.setEventHandler(FriendRejected.class, this::processRequestDeleted);
    return builder.build();
  }

  private CompletionStage<List<BoundStatement>> processFriendChanged(FriendAccepted event, UUID offset) {
    BoundStatement bindWriteFollowers = writeFollowers.bind();
    bindWriteFollowers.setString("userId", event.friendId.getUserId());
    bindWriteFollowers.setString("followedBy", event.userId.getUserId());

    BoundStatement bindDeleteRequester = deleteRequester.bind();
    bindDeleteRequester.setString("userId", event.friendId.getUserId());
    bindDeleteRequester.setString("requestedBy", event.userId.getUserId());

    BoundStatement bindWriteOffset = writeOffset.bind(offset);
    return completedStatements(Arrays.asList(bindWriteFollowers, bindDeleteRequester, bindWriteOffset));
  }

  private CompletionStage<List<BoundStatement>> processRequestAdded(FriendRequested event, UUID offset) {
    BoundStatement bindAddRequester = addRequester.bind();
    bindAddRequester.setString("userId", event.friendId.getUserId());
    bindAddRequester.setString("requestedBy", event.userId.getUserId());
    BoundStatement bindWriteOffset = writeOffset.bind(offset);
    return completedStatements(Arrays.asList(bindAddRequester, bindWriteOffset));
  }

  private CompletionStage<List<BoundStatement>> processRequestDeleted(FriendRejected event, UUID offset) {
    BoundStatement bindDeleteRequester = deleteRequester.bind();
    bindDeleteRequester.setString("userId", event.friendId.getUserId());
    bindDeleteRequester.setString("requestedBy", event.userId.getUserId());
    BoundStatement bindWriteOffset = writeOffset.bind(offset);
    return completedStatements(Arrays.asList(bindDeleteRequester, bindWriteOffset));
  }


}
