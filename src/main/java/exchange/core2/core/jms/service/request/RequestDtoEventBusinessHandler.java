package exchange.core2.core.jms.service.request;

/*import com.gxzx.margin.jms.msg.RequestDto;
import com.gxzx.margin.jms.msg.ResponseDto;
import com.gxzx.margin.jms.server.command.item.ItemAmountUpdateCommand;
import com.gxzx.margin.jms.server.command.order.OrderInsertCommand;
import com.gxzx.margin.jms.server.memdb.Item;
import com.gxzx.margin.jms.server.memdb.ItemRepository;
import com.gxzx.margin.mq.RabbitMqConfig;*/
import com.lmax.disruptor.EventHandler;
import exchange.core2.core.common.cmd.OrderCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理业务逻辑
 */
public class RequestDtoEventBusinessHandler implements EventHandler<OrderCommand> {

  /*private ItemRepository itemRepository;*/

  private static final Logger logger = LoggerFactory.getLogger(RequestDtoEventBusinessHandler.class);

  @Override
  public void onEvent(OrderCommand event, long sequence, boolean endOfBatch) throws Exception {
    //System.out.println(Thread.currentThread().getId() + " process RequestDtoEventBusinessHandler " + event + ", seq: " + sequence);
    /*if (event.hasErrorOrException()) {
      return;
    }*/

    /*RequestDto requestDto = event.getRequestDto();
    Item item = itemRepository.get(requestDto.getItemId());

    ResponseDto responseDto = new ResponseDto(requestDto.getId());

    if (item == null) {

      responseDto.setSuccess(false);
      responseDto.setErrorMessage("内存中还未缓存商品数据");
      logger.error("内存中还未缓存商品数据", item);

    } else if (item.decreaseAmount()) {

      responseDto.setSuccess(true);

      event.getCommandCollector().addCommand(
          new ItemAmountUpdateCommand(requestDto.getId(), item.getId(), item.getAmount())
      );
      event.getCommandCollector().addCommand(
          new OrderInsertCommand(requestDto.getId(), item.getId(), requestDto.getUserId())
      );

    } else {

      responseDto.setSuccess(false);
      responseDto.setErrorMessage("库存不足");
      logger.info("内存中还未缓存商品数据", item);
    }

    event.setResponseDto(responseDto);
*/

  }

  /*public void setItemRepository(ItemRepository itemRepository) {
    this.itemRepository = itemRepository;
  }*/

}
