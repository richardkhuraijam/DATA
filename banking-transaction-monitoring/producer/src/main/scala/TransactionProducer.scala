import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.util.Random
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TransactionProducer {

  def main(args: Array[String]): Unit = {

    val properties = new Properties()

    properties.put(
      "bootstrap.servers",
      "localhost:9092"
    )

    properties.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    properties.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer =
      new KafkaProducer[String, String](properties)

    val topic = "bank-transactions"

    val transactionTypes = Array(
      "UPI",
      "ATM",
      "POS",
      "MOBILE_BANKING",
      "INTERNET_BANKING"
    )

    val cities = Array(
      "Hyderabad",
      "Mumbai",
      "Delhi",
      "Bangalore",
      "Chennai",
      "Kolkata",
      "Pune"
    )

    val random = new Random()

    val formatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    var transactionNumber = 10001

    println("======================================")
    println(" Banking Transaction Producer Started ")
    println("======================================")
    println(s"Kafka Topic: $topic")
    println("Sending transactions every second...")
    println()

    try {

      while (true) {

        val transactionId =
          f"TX$transactionNumber%05d"

        val accountId =
          f"ACC${random.nextInt(20) + 101}%03d"

        val customerId =
          f"C${random.nextInt(20) + 101}%03d"

        val transactionType =
          transactionTypes(
            random.nextInt(transactionTypes.length)
          )

        val amount =
          500 + random.nextInt(150000)

        val city =
          cities(
            random.nextInt(cities.length)
          )

        val timestamp =
          LocalDateTime
            .now()
            .format(formatter)

        val json =
          s"""{"transaction_id":"$transactionId","account_id":"$accountId","customer_id":"$customerId","transaction_type":"$transactionType","amount":$amount,"city":"$city","timestamp":"$timestamp"}"""

        val record =
          new ProducerRecord[String, String](
            topic,
            accountId,
            json
          )

        producer.send(record)

        println(
          s"Sent: $transactionId | $accountId | $transactionType | ₹$amount | $city"
        )

        transactionNumber += 1

        Thread.sleep(1000)
      }

    } finally {

      producer.close()

      println("Producer stopped.")
    }
  }
}
