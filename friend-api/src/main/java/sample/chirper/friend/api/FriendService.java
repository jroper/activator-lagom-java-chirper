/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.chirper.friend.api;

import static com.lightbend.lagom.javadsl.api.Service.named;
import static com.lightbend.lagom.javadsl.api.Service.restCall;

import akka.NotUsed;
import com.lightbend.lagom.javadsl.api.Descriptor;
import com.lightbend.lagom.javadsl.api.Service;
import com.lightbend.lagom.javadsl.api.ServiceCall;
import com.lightbend.lagom.javadsl.api.deser.IdSerializers;
import com.lightbend.lagom.javadsl.api.transport.Method;
import org.pcollections.PSequence;
import sample.chirper.common.UserId;
import sample.chirper.common.UserIdentificationStrategy;

import java.util.Arrays;

/**
 * The friend service.
 */
public interface FriendService extends Service {

  /**
   * Service call for getting a user.
   *
   * The ID of this service call is the user name, and the response message is the User object.
   */
  ServiceCall<UserId, NotUsed, User> getUser();

  /**
   * Service call for creating a user.
   *
   * The request message is the User to create.
   */
  ServiceCall<NotUsed, User, NotUsed> createUser();

  /**
   * Service call for requesting a friend.
   */
  ServiceCall<FriendRequest, NotUsed, NotUsed> addFriendRequest();

  /**
   * Service call for accepting a friend request.
   */
  ServiceCall<FriendRequest, NotUsed, NotUsed> acceptFriendRequest();

  /**
   * Service call for rejecting a friend request.
   */
  ServiceCall<FriendRequest, NotUsed, NotUsed> rejectFriendRequest();

  /**
   * Service call for getting the followers of a user.
   *
   * The ID for this service call is the Id of the user to get the followers for.
   * The response message is the list of follower IDs.
   */
  ServiceCall<UserId, NotUsed, PSequence<UserId>> getFollowers();

  /**
   * Get the friend requests for a user.
   *
   * The ID for this service call is the Id of the user to get the friend requests for.
   * The response message is the list of friend request user IDs.
   */
  ServiceCall<UserId, NotUsed, PSequence<UserId>> getFriendRequests();

  @Override
  default Descriptor descriptor() {
    // @formatter:off
    return named("friendservice").with(
        restCall(Method.GET,    "/api/users/:id", getUser()),
        restCall(Method.POST,   "/api/users", createUser()),
        restCall(Method.PUT,    "/api/users/:userId/requests/:friendId", addFriendRequest()),
        restCall(Method.PUT,    "/api/users/:userId/friends/:friendId", acceptFriendRequest()),
        restCall(Method.DELETE, "/api/users/:userId/requests/:friendId", rejectFriendRequest()),
        restCall(Method.GET,    "/api/users/:id/followers", getFollowers()),
        restCall(Method.GET,    "/api/users/:id/requesters", getFriendRequests())
      ).withAutoAcl(true).with(UserId.class,
            IdSerializers.create("UserId", UserId::new, UserId::getUserId))
        .with(FriendRequest.class,
            IdSerializers.create("FriendRequest", FriendRequest::new, fr -> Arrays.asList(fr.userId, fr.friendId)))
        .withServiceIdentificationStrategy(UserIdentificationStrategy.INSTANCE);

    // @formatter:on
  }
}
