package com.indeed.dataengineering.models

case class Sales_Revenue_Summary(db: String, tbl: String, topic: String, partition: Int, offset: BigInt, year: Int, quarter: Int, user_id: BigInt, sales_revenue: BigInt, agency_revenue: BigInt, strategic_revenue: BigInt, sales_new_revenue: BigInt)