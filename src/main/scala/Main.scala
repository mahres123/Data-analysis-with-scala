import models._
import analysis._

object Main {
  def main(args: Array[String]): Unit = {
    // Step 1: Clean the data
    DataCleaner.cleanData()
    println("Data cleaning completed. Check the cleaned CSV files in the specified directory.")

    // Step 2: Load and display the cleaned data
    println("Loading cleaned data...")

    val products: List[Product] = DataLoader.loadProducts()
    val customers: List[Customer] = DataLoader.loadCustomers()
    val orders: List[Order] = DataLoader.loadOrders()
    val items: List[Item] = DataLoader.loadItems()

    // Display a summary of the data loaded
    println(s"Loaded ${products.size} products.")
    println(s"Loaded ${customers.size} customers.")
    println(s"Loaded ${orders.size} orders.")
    println(s"Loaded ${items.size} items.")

    // Display sample data
    println("\nSample Products:")
    products.take(5).foreach(println)

    println("\nSample Customers:")
    customers.take(5).foreach(println)

    println("\nSample Orders:")
    orders.take(5).foreach(println)

    println("\nSample Items:")
    items.take(5).foreach(println)

    // Step 3: Perform Sales Analysis
    println("\nPerforming Sales Analysis...")

    // Sales by customer, including order count and total spent
    val salesData: Map[String, (Int, Double)] = SalesAnalysis.salesByCustomer(orders, items)

    // Print Total Sales and Order Count by Customer (Sample)
    println("\nTotal Sales and Order Count by Customer:")
    salesData.take(5).foreach { case (customerId, (orderCount, totalSpent)) =>
      println(s"Customer ID: $customerId, Orders: $orderCount, Total Spent: $$$totalSpent")
    }

    // Identify and print top 5 customers by total spent
    val topCustomers = salesData.toList.sortBy(-_._2._2).take(5)
    println("\nTop 5 Best Clients by Total Spent:")
    topCustomers.foreach { case (customerId, (orderCount, totalSpent)) =>
      println(s"Customer ID: $customerId, Orders: $orderCount, Total Spent: $$$totalSpent")
    }

    // Calculate and print the number of recurring customers
    val recurringCustomers = salesData.filter { case (_, (orderCount, _)) => orderCount > 1 }
    println(s"\nNumber of recurring customers: ${recurringCustomers.size}")
    if (recurringCustomers.isEmpty) {
      println("No recurring customers found.")
    } else {
      println("Recurring customers found.")
    }

    // Calculate and print average basket value by category
    println("\nAverage Basket Value by Category:")
    val averageBasketByCategory = SalesAnalysis.averageBasketByCategory(products, items)
    averageBasketByCategory.foreach { case (category, avgBasketValue) =>
      println(s"Category: $category, Average Basket Value: $$$avgBasketValue")
    }

    // Calculate and print most popular products
    println("\nTop 10 Popular Products:")
    val popularProducts = SalesAnalysis.popularProducts(items)
    popularProducts.foreach { case (productId, purchaseCount) =>
      println(s"Product ID: $productId, Purchases: $purchaseCount")
    }

    // (Optional) Display popular products among recurring customers
    if (recurringCustomers.nonEmpty) {
      val recurringCustomerIds = recurringCustomers.keys.toSet
      val itemsByRecurringCustomers = orders
        .filter(order => recurringCustomerIds.contains(order.customerId))
        .flatMap(order => items.filter(_.orderId == order.orderId))

      val popularItemsByRecurringCustomers = itemsByRecurringCustomers
        .groupBy(_.productId)
        .view.mapValues(_.size)
        .toList.sortBy(-_._2)
        .take(10)

      println("\nTop Products Ordered by Recurring Customers:")
      popularItemsByRecurringCustomers.foreach { case (productId, count) =>
        println(s"Product ID: $productId, Purchases: $count")
      }
    }

    // Step 4: Perform Customer Segmentation
    println("\nPerforming Customer Segmentation...")

    val customerSegments: Map[String, String] = CustomerSegmentation.segmentCustomers(salesData)
    println("\nCustomer Segments:")
    customerSegments.take(10).foreach { case (customerId, segment) =>
      println(s"Customer ID: $customerId, Segment: $segment")
    }

    println("\nData analysis and segmentation completed.")
  }
}
