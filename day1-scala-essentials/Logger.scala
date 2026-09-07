trait Logger {
  def log(message: String): Unit
}

class ConsoleLogger extends Logger {
  def log(message: String): Unit = {
    println("Console: " + message)
  }
}

class FileLogger extends Logger {
  def log(message: String): Unit = {
    println("File: " + message)
  }
}

object LoggerDemo extends App {

  val consoleLogger = new ConsoleLogger()
  val fileLogger = new FileLogger()

  consoleLogger.log("Application started")
  fileLogger.log("Application started")
}
