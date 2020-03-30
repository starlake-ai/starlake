package com.ebiznext.comet.utils

import scopt.OParser

trait CliConfig[T] {
  def parser: OParser[Unit, T]
  def usage() = OParser.usage(parser)
  def parse(args: Seq[String]): Option[T]

}

