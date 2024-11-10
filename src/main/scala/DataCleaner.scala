import scala.io.Source
import java.io.PrintWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps

object DataCleaner {

  // Helper function to read CSV data
  private def readCSV(filePath: String): List[Array[String]] = {
    val bufferedSource = Source.fromFile(filePath)
    val data = bufferedSource.getLines().drop(1).map(_.split(",")).toList
    bufferedSource.close()
    data
  }

  // Function to remove duplicates and handle missing values 
  private def handleMissingAndDuplicates(data: List[Array[String]], default: String = "NA"): List[Array[String]] = {
    data
      .map(row => row.map(cell => if (cell == null || cell.isEmpty) default else cell))
      .distinct
  }

  // Function to standardize values (e.g., lowercase for consistency)
  private def standardizeValues(data: List[Array[String]], index: Int): List[Array[String]] = {
    data.map(row => {
      // Check if the row has enough columns to avoid IndexOutOfBoundsException
      if (row.length > index) {
        row.updated(index, row(index).toLowerCase())
      } else {
        row // Return the row unchanged if it doesn’t have enough columns
      }
    })
  }

  // Function to convert a date string column to LocalDateTime format if possible
  private def convertColumnToDate(data: List[Array[String]], index: Int, format: String = "yyyy-MM-dd HH:mm:ss"): List[Array[String]] = {
    val formatter = DateTimeFormatter.ofPattern(format)
    data.map(row => {
      // Check if the row has enough columns to avoid IndexOutOfBoundsException
      if (row.length > index) {
        val date = Try(LocalDateTime.parse(row(index), formatter)).getOrElse(null) // Set invalid dates to null
        row.updated(index, if (date != null) date.toString else "")
      } else {
        row // Return the row unchanged if it doesn’t have enough columns
      }
    })
  }

  // Function to write cleaned data to a CSV file
  private def writeCSV(filePath: String, data: List[Array[String]], headers: Array[String]): Unit = {
    val pw = new PrintWriter(new java.io.File(filePath))
    pw.println(headers.mkString(","))
    data.foreach(row => pw.println(row.mkString(",")))
    pw.close()
  }

  // Main cleaning function
  def cleanData(): Unit = {
    // File paths
    val customerPath = "C:\\Users\\Amina\\Desktop\\IMSD\\Scala\\customer.csv"
    val itemsPath = "C:\\Users\\Amina\\Desktop\\IMSD\\Scala\\items.csv"
    val ordersPath = "C:\\Users\\Amina\\Desktop\\IMSD\\Scala\\orders.csv"
    val productsPath = "C:\\Users\\Amina\\Desktop\\IMSD\\Scala\\products.csv"

    // Clean and save each file

    // Customer data
    val customerData = readCSV(customerPath)
    val customerHeaders = Array("customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state")
    val customerCleaned = handleMissingAndDuplicates(standardizeValues(customerData, 3))
    writeCSV("C:\\Users\\Amina\\Desktop\\IMSD\\Scala\\cleaned_customer.csv", customerCleaned, customerHeaders)

    // Items data
    val itemsData = readCSV(itemsPath)
    val itemsHeaders = Array("order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value")
    val itemsNoMissing = handleMissingAndDuplicates(itemsData)
    val itemsCleaned = convertColumnToDate(itemsNoMissing, 4) // Convert `shipping_limit_date`
    writeCSV("C:\\Users\\Amina\\Desktop\\IMSD\\Scala\\cleaned_items.csv", itemsCleaned, itemsHeaders)

    // Orders data
    val ordersData = readCSV(ordersPath)
    val ordersHeaders = Array("order_id", "customer_id", "order_status", "order_purchase_timestamp",
      "order_approved_at", "order_delivered_carrier_date",
      "order_delivered_customer_date", "order_estimated_delivery_date")
    val ordersNoMissing = handleMissingAndDuplicates(ordersData)
    val ordersDatesConverted = ordersNoMissing
      .pipe(data => convertColumnToDate(data, 3))
      .pipe(data => convertColumnToDate(data, 4))
      .pipe(data => convertColumnToDate(data, 5))
      .pipe(data => convertColumnToDate(data, 6))
      .pipe(data => convertColumnToDate(data, 7))
    writeCSV("C:\\Users\\Amina\\Desktop\\IMSD\\Scala\\cleaned_orders.csv", ordersDatesConverted, ordersHeaders)

    // Products data
    val productsData = readCSV(productsPath)
    val productsHeaders = Array("product_id", "product_category_name", "product_name_length",
      "product_description_length", "product_photos_qty", "product_weight_g",
      "product_length_cm", "product_height_cm", "product_width_cm",
      "product_category_name_english")
    val productsNoMissing = handleMissingAndDuplicates(productsData)
    val productsStandardized = standardizeValues(productsNoMissing, 1) // Standardize `product_category_name`
    writeCSV("C:\\Users\\Amina\\Desktop\\IMSD\\Scala\\cleaned_products.csv", productsStandardized, productsHeaders)
  }
}
