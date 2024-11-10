package analysis



object CustomerSegmentation {

  // Function to segment customers based on their total spending and order count
  def segmentCustomers(salesData: Map[String, (Int, Double)]): Map[String, String] = {
    salesData.map { case (customerId, (orderCount, totalSpent)) =>
      val segment = (orderCount, totalSpent) match {
        case (count, amount) if count > 5 && amount > 1000 => "VIP"
        case (count, amount) if count > 2 && amount > 500  => "Loyal"
        case _                                             => "Occasional"
      }
      customerId -> segment
    }
  }
}


