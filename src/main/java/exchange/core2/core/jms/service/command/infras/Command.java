package exchange.core2.core.jms.service.command.infras;




import java.io.Serializable;
import java.util.UUID;

/**
 * 数据库操作命令
 */
public abstract class Command implements Serializable {

  private static final long serialVersionUID = -2463630580877588711L;
  protected final String id;

  protected final String requestId;

  /**
   * Command来源的requestId
   *
   * @param requestId
   */
  public Command(String requestId) {
    this.id = UUID.randomUUID().toString(); //ul StrongUuidGenerator.getNextId();
    this.requestId = requestId;
  }

  /**
   * 全局唯一Id, uuid
   *
   * @return
   */
  public String getId() {
    return id;
  }

  /**
   * 对应的{@link RequestDto#id}
   *
   * @return
   */
  public String getRequestId() {
    return requestId;
  }
}
