//Total loaded data size:
//14 nodes
//10 relationships

//Load receipts with related customers and products
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/JMHReif/data-normalization/main/sales_receipts_1NF/sales_receipts_1NF.csv" AS row
WITH row
MERGE (r:Receipt {transactionId: row.transaction_id})
ON CREATE SET r.transactionDate = date(row.transaction_date),
    r.transactionTime = localtime(row.transaction_time),
    r.inStore = row.instore_yn
WITH row, r
MERGE (c:Customer {loyaltyNumber: row.loyalty_num})
ON CREATE SET c.name = row.customer
MERGE (c)-[rel:MADE_PURCHASE]->(r)
WITH row, r
MERGE (p:Product {productName: row.product})
MERGE (r)-[rel2:CONTAINS {lineItemId: row.line_item_id}]->(p)
ON CREATE SET rel2.quantity = row.quantity,
    rel2.unitPrice = row.unit_price,
    rel2.promoItem = row.promo_item_yn
RETURN count(r);