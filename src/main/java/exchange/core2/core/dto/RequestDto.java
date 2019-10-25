package exchange.core2.core.dto;

import exchange.core2.core.base.msg.MessageDto;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * 购买商品的请求
 */
public class RequestDto extends MessageDto {

  public byte[] payLoad;

  public byte[] getPayLoad() {
    return payLoad;
  }

  public void setPayLoad(byte[] payLoad) {
    this.payLoad = payLoad;
  }

  private static final long serialVersionUID = 5515305970509119810L;
  /**
   * 商品ID
   */
  private Long itemId;

  /**
   * 用户ID
   */
  private String userId;


  public RequestDto(Long itemId, String userId) {
    super();
    this.itemId = itemId;
    this.userId = userId;
  }

  public Long getItemId() {
    return itemId;
  }

  public String getUserId() {
    return userId;
  }

  public void setItemId(Long itemId) {
    this.itemId = itemId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RequestDto that = (RequestDto) o;

    if (itemId != null ? !itemId.equals(that.itemId) : that.itemId != null) return false;
    return userId != null ? userId.equals(that.userId) : that.userId == null;
  }

  @Override
  public int hashCode() {
    int result = itemId != null ? itemId.hashCode() : 0;
    result = 31 * result + (userId != null ? userId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("itemId", itemId)
        .append("userId", userId)
        .toString();
  }
}
