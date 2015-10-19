package quizes

/**
 * @author eslam
 */
object Quiz2 extends App {
  val x = 1 to 100
  x.foreach {
    case i if ((i % 5 == 0) && (i % 3 == 0)) => println("FizzBuzz")
    case k if (k % 3 == 0) => println("Fizz")
    case l if (l % 5 == 0) => println("Buzz")
    case n => println(n)
    }
   
}

