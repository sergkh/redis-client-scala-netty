package com.fotolog.redis.utils

import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions.{IncrementOptions, ModifyOpts, ResultOptions}

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 03.03.2017.
  */
object SortedSetOptions {

  case class ZaddOptions(
                          modifyOpts: Option[ModifyOpts] = None,
                          resultOpts: Option[ResultOptions] = None,
                          incOpt: Option[IncrementOptions] = None
                        ) {

    def asBin: Seq[Array[Byte]] = {
      Seq(modifyOpts.map(_.asBin), resultOpts.map(_.asBin), incOpt.map(_.asBin)).collect {
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

    object INCR extends IncrementOptions

    class ResultOptions {
      def asBin = "CH".getBytes
    }

    class IncrementOptions {
      def asBin = "INCR".getBytes
    }

    def apply(): ZaddOptions = new ZaddOptions(None, None, None)
  }

}
