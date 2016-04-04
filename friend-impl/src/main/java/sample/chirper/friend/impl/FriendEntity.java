/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.chirper.friend.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.lightbend.lagom.javadsl.persistence.PersistentEntity;

import akka.Done;
import org.pcollections.HashTreePSet;
import sample.chirper.friend.api.User;
import sample.chirper.common.UserId;
import sample.chirper.friend.impl.FriendCommand.CreateUser;
import sample.chirper.friend.impl.FriendCommand.GetUser;
import sample.chirper.friend.impl.FriendCommand.GetUserReply;
import sample.chirper.friend.impl.FriendEvent.UserCreated;

public class FriendEntity extends PersistentEntity<FriendCommand, FriendEvent, FriendState> {

  @Override
  public Behavior initialBehavior(Optional<FriendState> snapshotState) {

    BehaviorBuilder b = newBehaviorBuilder(snapshotState.orElse(
      new FriendState(Optional.empty(), HashTreePSet.empty())));

    b.setCommandHandler(CreateUser.class, (cmd, ctx) -> {
      if (state().user.isPresent()) {
        ctx.invalidCommand("User " + entityId() + " is already created");
        return ctx.done();
      } else {
        User user = cmd.user;
        List<FriendEvent> events = new ArrayList<FriendEvent>();
        events.add(new UserCreated(user.userId, user.name));
        for (UserId friendId : user.friends) {
          events.add(new FriendEvent.FriendRequested(user.userId, friendId));
        }
        return ctx.thenPersistAll(events, () -> ctx.reply(Done.getInstance()));
      }
    });

    b.setEventHandler(UserCreated.class,
        evt -> new FriendState(Optional.of(new User(evt.userId, evt.name)), HashTreePSet.empty()));

    b.setCommandHandler(FriendCommand.RequestAddFriend.class, (cmd, ctx) -> {
      if (!state().user.isPresent()) {
        ctx.invalidCommand("User " + entityId() + " is not  created");
        return ctx.done();
      } else if (state().user.get().friends.contains(cmd.friendUserId) || state().friendRequests.contains(cmd.friendUserId)) {
        ctx.reply(Done.getInstance());
        return ctx.done();
      } else {
        return ctx.thenPersist(new FriendEvent.FriendRequested(getUserId(), cmd.friendUserId), evt ->
          ctx.reply(Done.getInstance()));
      }
    });

    b.setEventHandler(FriendEvent.FriendRequested.class, evt -> state().addFriendRequest(evt.friendId));

    b.setCommandHandler(FriendCommand.AcceptAddFriend.class, (cmd, ctx) -> {
      if (!state().user.isPresent()) {
        ctx.invalidCommand("User " + entityId() + " is not  created");
        return ctx.done();
      } else if (state().user.get().friends.contains(cmd.friendUserId) || !state().friendRequests.contains(cmd.friendUserId)) {
        ctx.reply(Done.getInstance());
        return ctx.done();
      } else {
        return ctx.thenPersist(new FriendEvent.FriendAccepted(getUserId(), cmd.friendUserId), evt ->
                ctx.reply(Done.getInstance()));
      }
    });

    b.setEventHandler(FriendEvent.FriendAccepted.class, evt -> state().acceptFriendRequest(evt.friendId));

    b.setCommandHandler(FriendCommand.RejectAddFriend.class, (cmd, ctx) -> {
      if (!state().user.isPresent()) {
        ctx.invalidCommand("User " + entityId() + " is not  created");
        return ctx.done();
      } else if (!state().friendRequests.contains(cmd.friendUserId)) {
        ctx.reply(Done.getInstance());
        return ctx.done();
      } else {
        return ctx.thenPersist(new FriendEvent.FriendRejected(getUserId(), cmd.friendUserId), evt ->
                ctx.reply(Done.getInstance()));
      }
    });

    b.setEventHandler(FriendEvent.FriendRejected.class, evt -> state().rejectFriendRequest(evt.friendId));

    b.setReadOnlyCommandHandler(GetUser.class, (cmd, ctx) -> {
      ctx.reply(new GetUserReply(state().user));
    });

    return b.build();
  }

  private UserId getUserId() {
    return state().user.get().userId;
  }
}
