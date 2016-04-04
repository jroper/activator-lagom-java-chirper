package sample.chirper.like.impl;

import akka.NotUsed;
import com.lightbend.lagom.javadsl.api.ServiceCall;
import com.lightbend.lagom.javadsl.persistence.PersistentEntityRef;
import com.lightbend.lagom.javadsl.persistence.PersistentEntityRegistry;
import org.pcollections.PCollection;
import sample.chirper.common.UserId;
import sample.chirper.like.api.ChirpId;
import sample.chirper.like.api.LikeChirp;
import sample.chirper.like.api.LikeService;

import javax.inject.Inject;

public class LikeServiceImpl implements LikeService {

  private final PersistentEntityRegistry persistentEntities;

  @Inject
  public LikeServiceImpl(PersistentEntityRegistry persistentEntities) {
    this.persistentEntities = persistentEntities;

    persistentEntities.register(LikeEntity.class);

  }

  @Override
  public ServiceCall<LikeChirp, NotUsed, NotUsed> likeChirp() {
    return (likeChirp, request) ->
      likeEntityRef(likeChirp.chirpId).ask(new LikeCommand.Like(likeChirp.liker))
        .thenApply(a -> NotUsed.getInstance());
  }

  @Override
  public ServiceCall<LikeChirp, NotUsed, NotUsed> unlikeChirp() {
    return (likeChirp, request) ->
        likeEntityRef(likeChirp.chirpId).ask(new LikeCommand.UnLike(likeChirp.liker))
          .thenApply(a -> NotUsed.getInstance());
  }

  @Override
  public ServiceCall<ChirpId, NotUsed, PCollection<UserId>> getLikes() {
    return (chirpId, request) ->
        likeEntityRef(chirpId).ask(new LikeCommand.GetLikes());
  }

  private PersistentEntityRef<LikeCommand> likeEntityRef(ChirpId chirpId) {
    PersistentEntityRef<LikeCommand> ref = persistentEntities.refFor(LikeEntity.class, chirpId.uuid);
    return ref;
  }

}
