import scala.io.Source
import scala.util.Using
import scala.util.Try 
import models._

object DataLoader {
  // Set the path to the location of your cleaned data files
  private val dataPath = "C:/Users/Amina/Desktop/IMSD/Scala"

  def loadProducts(): List[Product] = {
    Using(Source.fromFile(s"$dataPath/cleaned_products.csv")) { source =>
      source.getLines().drop(1).flatMap { line =>
        val columns = line.split(",").map(_.trim)
        if (columns.length >= 10) {
          try {
            val productId = columns(0)
            val categoryName = if (columns(1).isEmpty) "Unknown" else columns(1)
            val nameLength = if (columns(2).isEmpty) 0.0 else columns(2).toDouble
            val descriptionLength = if (columns(3).isEmpty) 0.0 else columns(3).toDouble
            val photosQty = if (columns(4).isEmpty) 0.0 else columns(4).toDouble
            val weightG = if (columns(5).isEmpty) 0.0 else columns(5).toDouble
            val lengthCm = if (columns(6).isEmpty) 0.0 else columns(6).toDouble
            val heightCm = if (columns(7).isEmpty) 0.0 else columns(7).toDouble
            val widthCm = if (columns(8).isEmpty) 0.0 else columns(8).toDouble
            val categoryNameEnglish = if (columns(9).isEmpty) "Unknown" else columns(9)

            Some(Product(productId, categoryName, nameLength, descriptionLength, photosQty, weightG, lengthCm, heightCm, widthCm, categoryNameEnglish))
          } catch {
            case e: Exception =>
              println(s"Error parsing line: $line. Error: ${e.getMessage}")
              None
          }
        } else {
          println(s"Skipping malformed line (not enough columns): $line")
          None
        }
      }.toList
    }.getOrElse(List.empty)
  }

  def loadCustomers(): List[Customer] = {
    Using(Source.fromFile(s"$dataPath/cleaned_customer.csv")) { source =>
      source.getLines().drop(1).flatMap { line =>
        val columns = line.split(",").map(_.trim)
        if (columns.length >= 5) {
          try {
            val customerId = columns(0)
            val customerUniqueId = columns(1)
            val zipCode = columns(2)
            val city = columns(3)
            val state = columns(4)
            Some(Customer(customerId, customerUniqueId, zipCode, city, state))
          } catch {
            case e: Exception =>
              println(s"Error parsing line: $line. Error: ${e.getMessage}")
              None
          }
        } else {
          println(s"Skipping malformed line (not enough columns): $line")
          None
        }
      }.toList
    }.getOrElse(List.empty)
  }

  def loadOrders(): List[Order] = {
    Using(Source.fromFile(s"$dataPath/cleaned_orders.csv")) { source =>
      source.getLines().drop(1).flatMap { line =>
        val columns = line.split(",").map(_.trim)
        if (columns.length >= 8) {
          try {
            val orderId = columns(0)
            val customerId = columns(1)
            val orderStatus = columns(2)
            val orderPurchaseTimestamp = columns(3)
            val orderApprovedAt = columns(4)
            val orderDeliveredCarrierDate = columns(5)
            val orderDeliveredCustomerDate = columns(6)
            val orderEstimatedDeliveryDate = columns(7)
            Some(Order(orderId, customerId, orderStatus, orderPurchaseTimestamp, orderApprovedAt, orderDeliveredCarrierDate, orderDeliveredCustomerDate, orderEstimatedDeliveryDate))
          } catch {
            case e: Exception =>
              println(s"Error parsing line: $line. Error: ${e.getMessage}")
              None
          }
        } else {
          println(s"Skipping malformed line (not enough columns): $line")
          None
        }
      }.toList
    }.getOrElse(List.empty)
  }

  def loadItems(): List[Item] = {
    val itemFilePath = s"$dataPath/cleaned_items.csv"
    println(s"Loading items from: $itemFilePath")

    Using(Source.fromFile(itemFilePath)) { source =>
      source.getLines().drop(1).flatMap { line =>
        println(s"Reading line: $line")

        // Split columns by comma, handling extra quotes
        val columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.replaceAll("^\"|\"$", "").trim)

        if (columns.length >= 7) {
          try {
            val orderId = columns(0)
            val orderItemId = columns(1).toInt
            val productId = columns(2)
            val sellerId = columns(3)
            val shippingLimitDate = columns(4)

            // Use Double.NaN for missing or non-numeric price and freight values
            val price = Try(columns(5).toDouble).getOrElse(Double.NaN)
            val freightValue = Try(columns(6).toDouble).getOrElse(Double.NaN)

            Some(Item(orderId, orderItemId, productId, sellerId, shippingLimitDate, price, freightValue))
          } catch {
            case e: Exception =>
              println(s"Error parsing line: $line. Error: ${e.getMessage}")
              None
          }
        } else {
          println(s"Skipping malformed line (not enough columns): $line")
          None
        }
      }.toList
    }.getOrElse(List.empty)
  }
}
