package models

case class Order(orderId: String, customerId: String, orderStatus: String,
                 orderPurchaseTimestamp: String, orderApprovedAt: String,
                 orderDeliveredCarrierDate: String, orderDeliveredCustomerDate: String,
                 orderEstimatedDeliveryDate: String)


