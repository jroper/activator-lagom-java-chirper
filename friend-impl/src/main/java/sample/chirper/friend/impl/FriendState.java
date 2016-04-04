/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.chirper.friend.impl;

import java.util.Optional;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.pcollections.HashTreePSet;
import org.pcollections.PSequence;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.lightbend.lagom.serialization.Jsonable;

import org.pcollections.PSet;
import sample.chirper.friend.api.User;
import sample.chirper.common.UserId;

@SuppressWarnings("serial")
@Immutable
public final class FriendState implements Jsonable {

  public final Optional<User> user;
  public final PSet<UserId> friendRequests;

  @JsonCreator
  public FriendState(Optional<User> user, PSet<UserId> friendRequests) {
    this.user = Preconditions.checkNotNull(user, "user");
    this.friendRequests = friendRequests == null ? HashTreePSet.empty() : friendRequests;
  }

  public FriendState acceptFriendRequest(UserId friendUserId) {
    if (!user.isPresent())
      throw new IllegalStateException("friend can't be added before user is created");
    PSet<UserId> newRequests = friendRequests.minus(friendUserId);
    PSequence<UserId> newFriends = user.get().friends.plus(friendUserId);
    return new FriendState(Optional.of(new User(user.get().userId, user.get().name, Optional.of(newFriends))), newRequests);
  }

  public FriendState rejectFriendRequest(UserId friendUserId) {
    if (!user.isPresent())
      throw new IllegalStateException("friend can't be rejected before user is created");
    PSet<UserId> newRequests = friendRequests.minus(friendUserId);
    return new FriendState(user, newRequests);
  }

  public FriendState addFriendRequest(UserId friendUserId) {
    if (!user.isPresent())
      throw new IllegalStateException("friend request can't be added before user is created");
    PSet<UserId> newRequests = friendRequests.plus(friendUserId);
    return new FriendState(user, newRequests);
  }

  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another)
      return true;
    return another instanceof FriendState && equalTo((FriendState) another);
  }

  private boolean equalTo(FriendState another) {
    return user.equals(another.user);
  }

  @Override
  public int hashCode() {
    int h = 31;
    h = h * 17 + user.hashCode();
    return h;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("FriendState").add("user", user).toString();
  }
}
