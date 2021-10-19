
object DockerLaunch extends App {
  val lowerBound = if (args.isEmpty) "20210904" else args(0)
  val upperBound = if (args.isEmpty) "20210906" else args(1)
  println("Successes")

  UserFeatchers.start(lowerBound, upperBound, "/opt/spark-data")
}