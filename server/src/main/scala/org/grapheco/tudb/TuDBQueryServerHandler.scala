/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler}
import org.grapheco.tudb.facade.GraphFacade

import java.text.SimpleDateFormat

/** Handles a server-side channel.
  */
@Sharable
class TuDBQueryServerHandler(db: GraphFacade) extends SimpleChannelInboundHandler[String]() {
  val objectMapper = new ObjectMapper()
    .findAndRegisterModules()
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    // 持续时间序列化为字符串
    .configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false)
    // 当出现 Java 类中未知的属性时不报错，而是忽略此 JSON 字段
    .configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
    // 枚举类型调用 `toString` 方法进行序列化
    .configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
    // 设置 java.util.Date 类型序列化格式
    .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))

//  override def channelActive(ctx: ChannelHandlerContext) {
//    // Send greeting for a new connection.
//    ctx.write("Welcome to " + InetAddress.getLocalHost().getHostName() + "!\r\n")
//    ctx.write("It is " + new Date() + " now.\r\n")
//    ctx.flush()
//  }

  override def channelRead0(ctx: ChannelHandlerContext, request: String) {
    var response: String = null

    val queryResultIter = db.cypher(request).records()
    if (!queryResultIter.hasNext) {
      response = ""
    } else {
      response = objectMapper.writeValueAsString(queryResultIter.toList)
    }

    val future = ctx.write(response)

  }

  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close()
  }
}
