package analysis

import models._

object SalesAnalysis {
  // Calculate total sales and order count by customer
  def salesByCustomer(orders: List[Order], items: List[Item]): Map[String, (Int, Double)] = {
    val orderItemsMap = items.groupBy(_.orderId)  // Create a map to look up items by orderId for efficiency
    orders.groupBy(_.customerId).map { case (customerId, customerOrders) =>
      val totalSpent = customerOrders.flatMap(order => orderItemsMap.getOrElse(order.orderId, Nil).map(_.price)).sum
      val orderCount = customerOrders.size
      customerId -> (orderCount, totalSpent)
    }
  }

  // Calculate average basket value by product category
  def averageBasketByCategory(products: List[Product], items: List[Item]): Map[String, Double] = {
    val productItemsMap = items.groupBy(_.productId)  // Create a map to look up items by productId for efficiency
    products.groupBy(_.categoryName).map { case (category, products) =>
      val totalRevenue = products.flatMap(product => productItemsMap.getOrElse(product.productId, Nil).map(_.price)).sum
      val totalOrders = products.flatMap(product => productItemsMap.getOrElse(product.productId, Nil)).size
      category -> (if (totalOrders > 0) totalRevenue / totalOrders else 0.0)
    }
  }

  // Identify the most popular products
  def popularProducts(items: List[Item]): List[(String, Int)] = {
    items.groupBy(_.productId).map { case (productId, productItems) =>
      productId -> productItems.size
    }.toList.sortBy(-_._2).take(10)
  }
}
