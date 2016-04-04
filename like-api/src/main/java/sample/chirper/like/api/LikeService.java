package sample.chirper.like.api;

import static com.lightbend.lagom.javadsl.api.Service.named;
import static com.lightbend.lagom.javadsl.api.Service.restCall;

import akka.NotUsed;
import com.lightbend.lagom.javadsl.api.Descriptor;
import com.lightbend.lagom.javadsl.api.Service;
import com.lightbend.lagom.javadsl.api.ServiceCall;
import com.lightbend.lagom.javadsl.api.deser.IdSerializers;
import com.lightbend.lagom.javadsl.api.transport.Method;
import org.pcollections.PCollection;
import org.pcollections.PSequence;
import sample.chirper.common.UserId;
import sample.chirper.common.UserIdentificationStrategy;

import java.util.Arrays;

/**
 * The friend service.
 */
public interface LikeService extends Service {

  /**
   * Like a Chirp.
   */
  ServiceCall<LikeChirp, NotUsed, NotUsed> likeChirp();

  /**
   * Unlike a Chirp.
   */
  ServiceCall<LikeChirp, NotUsed, NotUsed> unlikeChirp();

  /**
   * Get all the users that like a chirp.
   */
  ServiceCall<ChirpId, NotUsed, PCollection<UserId>> getLikes();

  @Override
  default Descriptor descriptor() {
    // @formatter:off
    return named("likeservice").with(
        restCall(Method.PUT,    "/api/chirps/:userId/:uuid/likes/:likerId", likeChirp()),
        restCall(Method.DELETE, "/api/chirps/:userId/:uuid/likes/:likerId", unlikeChirp()),
        restCall(Method.GET,   "/api/chirps/:userId/:uuid/likes", getLikes())
    ).withAutoAcl(true)
        .with(UserId.class, IdSerializers.create("UserId", UserId::new, UserId::getUserId))
        .with(ChirpId.class, IdSerializers.create("ChirpId", ChirpId::new, c -> Arrays.asList(c.userId, c.uuid)))
        .with(LikeChirp.class, IdSerializers.create("LikeChirp", LikeChirp::new, l -> Arrays.asList(l.chirpId, l.liker)))
        .withServiceIdentificationStrategy(UserIdentificationStrategy.INSTANCE);
    // @formatter:on
  }
}
