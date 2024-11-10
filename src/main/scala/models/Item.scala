package models

case class Item(orderId: String, orderItemId: Int, productId: String, sellerId: String,
                shippingLimitDate: String, price: Double, freightValue: Double)


