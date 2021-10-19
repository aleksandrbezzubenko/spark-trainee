package recap

import scala.annotation.tailrec

object Exercises extends App {
  /*
    Basics
    1. Concatenate a string n times
    2. Fibonacci function, tail recursive.
    3. IsPrime function tail recursive
   */

  // 1
  val concatenateN = "abc" * 10

  // 2
  @tailrec
  def FibonacciFunc(index: Int, prev: Int = 1, current: Int = 0): Int = {
    if (index <= 0) current
    else FibonacciFunc(index - 1, prev = prev + current, current = prev)
  }
  val fib7 = FibonacciFunc(7)

  // 3
  def isPrime(n: Int): Boolean = {
    @tailrec
    def isPrimeTailrec(divisor: Int): Boolean = {
      if(divisor > Math.sqrt(Math.abs(n))) true
      else n % divisor != 0 && isPrimeTailrec(divisor + 1)
    }
    if(n < 2) false
    else isPrimeTailrec(2)
  }

  /*
    Functions
    1.  a function which takes 2 strings and concatenates them
    2.  define a function which takes an int and returns another function which takes an int and returns an int
        - what's the type of this function  Int => Int
        - how to do it(carrying?) yes
  */

  // 1
  val concatenate2 = (str1: String, str2: String) => str1 + str2
  // 2
  def multVal(value1: Int): Int => Int = {
    (value2: Int) => value1 * value2
  }
  val mult = multVal(3)
  val mult1 = mult(5)

  /*
  Collections (ONLY IMMUTABLE!!!)
  Overly simplified social network based on maps(Map[String, Set[String]] where key is user and value is Set of friends)
      Person = String
      - add a person to the network
      - remove
      - friend (mutual)
      - unfriend
      - number of friends of a person
      - person with most friends
      - how many people have NO friends
      - if there is a social connection between two people (direct or not)
   */
  class SocialNetwork(val persons: Map[String, Set[String]] = Map()) {
    def AddPerson(name: String, friends: Set[String] = Set()): SocialNetwork = {
      new SocialNetwork(persons + (name -> friends))
    }
    def Remove(name: String): Map[String, Set[String]] = {
      persons - name
    }
    def AddFriend(namePerson: String, nameFriend: String): SocialNetwork = {
      if (persons.contains(nameFriend)) {
        new SocialNetwork(persons
          .updated(namePerson, persons(namePerson) + nameFriend)
          .updated(nameFriend, persons(nameFriend) + namePerson)
        )
      } else {
        new SocialNetwork(persons
          .updated(namePerson, persons(namePerson) + nameFriend) + (nameFriend -> Set(namePerson))
        )
      }
    }
    def RemoveFriend(namePerson: String, nameFriend: String): SocialNetwork = {
      if (persons.contains(nameFriend)) {
        new SocialNetwork(persons
          .updated(namePerson, persons(namePerson) - nameFriend)
          .updated(nameFriend, persons(nameFriend) - namePerson)
        )
      } else {
        println("There is no such friend!")
        this
      }
    }
    def NumberOfFriends(name: String): Int = persons(name).size
    def MostFriends(): (String, Set[String]) = persons.maxBy(_._2.size)
    def CountOfNoFriends(): Int = persons.count(_._2.isEmpty)
    def SocialNoDirectConnection(person1: String, person2: String): Boolean = {
      if (persons(person1).contains(person2)) true
      else {
        persons(person1).forall(p => SocialNoDirectConnection(p, person2))
      }
    }
    def SocialConnection(person1: String, person2: String, links: Map[String, Set[String]] = persons): (Boolean, Boolean) = {
      if (persons(person2).isEmpty) (false, false)
      else if (persons(person1).contains(person2) && links == persons) (true, true)
      else if (persons(person1).contains(person2) && links != persons) (true, false)
      else {
        val newLinks = links - person1
        var flag = false
        for (p <- persons(person1)){
          if (newLinks.contains(p)) {
            if (SocialConnection(p, person2, newLinks)._1) flag = true
          }
        }
        (flag, false)
      }
    }
  }

  // Testing Social network
  val socNet = new SocialNetwork()
    .AddPerson("Sasha")
    .AddPerson("Denis")
    .AddPerson("Petr")
    .AddPerson("Artem")
    .AddPerson("Mishail")
    .AddPerson("Anton")
    .AddPerson("Oleg")
    .AddPerson("Andrew")
    .AddPerson("Pavel")
    .AddFriend("Sasha", "Denis")
    .AddFriend("Sasha", "Petr")
    .AddFriend("Sasha", "Artem")
    .AddFriend("Artem", "Mishail")
    .AddFriend("Mishail", "Anton")
  println(socNet.SocialConnection("Sasha", "Anton"))
  println(socNet.MostFriends())
  println(socNet.CountOfNoFriends())

}
