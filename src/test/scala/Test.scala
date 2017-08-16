object Test {

  def main(args: Array[String]): Unit = {
    val list = List(1,2,4,5,3)
    for (i <- list) println("Array Element: " + i)
    println("--------------------------")
//    for (i <- isort(list)) println("Array Element: " + i)
    println("--------------------------")
    for (i <- insert(2,list)) println("Array Element: " + i)


  }

  //  排序
  def isort(xs: List[Int]): List[Int] =
    if (xs.isEmpty) Nil
    else insert(xs.head, isort(xs.tail))


  def insert(x: Int, xs: List[Int]): List[Int] =
    if (xs.isEmpty || x <= xs.head) x :: xs
    else xs.head :: insert(x, xs.tail)

}
