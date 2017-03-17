package com.fotolog.redis.utils

import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions.ModifyOpts

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 03.03.2017.
  */
object SortedSetOptions {

  case class ZaddOptions(
                          modifyOpts: Option[ModifyOpts] = None,
                          withResultOpts: Boolean = false,
                          withIncOpt: Boolean = false
                        ) {

    def asBin: Seq[Array[Byte]] = {
      val resultOptionsBytes =  if (withResultOpts) Some("CH".getBytes) else None
      val incOptionsBytes = if (withIncOpt) Some("INCR".getBytes) else None
      Seq(modifyOpts.map(_.asBin), resultOptionsBytes, incOptionsBytes).collect {
        case Some(bytes) => bytes
      }
    }
  }

  object ZaddOptions {
    sealed class ModifyOpts(name: String) {
      def asBin = name.getBytes
    }

    object NX extends ModifyOpts("NX")

    object XX extends ModifyOpts("XX")

  }

  sealed class Agregation(name: String) {
    def asBin = name.getBytes
  }

  case object SumAgregation extends Agregation("SUM")
  case object MinAgregation extends Agregation("MIN")
  case object MaxAgregation extends Agregation("MAX")
  
}
