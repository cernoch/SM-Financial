package cernoch.sm.financial

import cernoch.scalogic._
import java.lang.String
import collection.immutable.Map
import java.io.InputStreamReader
import storage.Dumpable
import util.parsing.input.StreamReader
import collection.{Iterable, Iterator}
import java.text.{SimpleDateFormat, DateFormat}

object Financial extends Dumpable[BLC[Atom[Val[_]]]] {

  val domain = Financial.lines("domains.txt")
    .filter(_.trim.length > 0)
    .map{ Domain(_) }
    .toSet

  val schema = Financial.lines("model.txt")
    .filter(_.trim.length > 0) // Remove empty lines
    .map{ Btom(_,domain) }
    .map{ BLC(_) }
    .toSet

  def dump
  : Iterable[BLC[Atom[Val[_]]]]
  = for ((atom, data) <- raw.view; line <- data.view) yield {

      def strDom(d:Domain[_], s:String) : Val[_] = try {
        d match {
          case d@NumDom("date",_) => new Num(
            Financial.DATE_FORMATTER.parse(s).getTime() / 1000 / 3600 / 24,
            d)

          case d@DecDom(_)     => new Dec(d.valueOf(s),d)
          case d@NumDom(_,_)   => new Num(d.valueOf(s),d)
          case d@CatDom(_,_,_) => new Cat(d.valueOf(s),d)
        }
      } catch {
        case e:NumberFormatException => throw new IllegalArgumentException(
          "Value '" + s + "' is not a number for domain '" + d + "'.", e)
      }

      def f(x:List[Var], y:List[String]) :List[Val[_]] = (x,y) match {
        case (Nil, Nil) => List()
        case (value :: vTail, str :: sTail) => value.dom match {
          case d@DecDom(_)     => strDom(d,str) :: f(vTail,sTail)
          case d@NumDom(_,_)   => strDom(d,str) :: f(vTail,sTail)
          case d@CatDom(_,_,_) => strDom(d,str) :: f(vTail,sTail)
        }
        case (v :: t, Nil) => f(v :: t, List("")) //pad missing values
      }
      BLC(new Atom(atom.head.pred, f(atom.head.args, line)))
    }

  def raw = schema.foldLeft(
    Map[ BLC[Btom[Var]], Iterable[List[String]] ]() )(
    (map, a) => { map + (a -> i2i {
      skip(Financial.lines(a.head.pred + ".asc")).map {
        _.split(";").map(field =>
            if (field.charAt(0) != '"') field
            else field.substring(1,field.length-1)
        ).toList
      }
    })}
  )

  private def skip[T](i:Iterator[T]) = {i.next(); i}
  private def i2i[T](f: => Iterator[T]) = new Iterable[T] { def iterator = f }

  private def DATE_FORMATTER = new SimpleDateFormat("yyMMdd")

  private def lines(name:String) = source(name) match {
    case None => throw new Exception("File not found.")
    case Some(v) => v.getLines
  }

  private def source(name:String) = stream(name) match {
    case Some(v) => Some(io.Source.fromInputStream(v))
    case None => None
  }

  private def reader(name:String) = stream(name) match {
    case Some(v) => Some(StreamReader(new InputStreamReader(v)))
    case None => None
  }

  private def stream(name: String) = {
    val fname = "cernoch/sm/financial/data/" + name
    val stream = getClass.getClassLoader.getResourceAsStream(fname)
    if (stream == null) None else Some(stream)
  }

  def main(args: Array[String])
  = Financial.dump.foreach(println)
}
