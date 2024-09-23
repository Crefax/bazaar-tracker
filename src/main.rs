use mongodb::{Client, options::ClientOptions, bson::doc};
use serde::{Deserialize, Serialize};
use chrono::Utc;
use tokio::time::{sleep, Duration};
use reqwest;
use std::error::Error;
use bson::{DateTime as BsonDateTime, Document};

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ProductSummary {
    amount: i64,
    pricePerUnit: f64,
    orders: i32,
}

#[derive(Deserialize, Serialize, Debug)]
struct Product {
    product_id: String,
    sell_summary: Vec<ProductSummary>,
    buy_summary: Vec<ProductSummary>,
    quick_status: QuickStatus,
}

#[derive(Deserialize, Serialize, Debug)]
struct QuickStatus {
    productId: String,
    sellPrice: f64,
    sellVolume: i64,
    sellMovingWeek: i64,
    sellOrders: i32,
    buyPrice: f64,
    buyVolume: i64,
    buyMovingWeek: i64,
    buyOrders: i32,
}

#[derive(Deserialize, Debug)]
struct ApiResponse {
    success: bool,
    lastUpdated: i64,
    products: std::collections::HashMap<String, Product>,
}

async fn fetch_bazaar_data() -> Result<ApiResponse, Box<dyn Error>> {
    let url = "https://api.hypixel.net/skyblock/bazaar";
    let response = reqwest::get(url).await?.json::<ApiResponse>().await?;
    Ok(response)
}

async fn save_to_mongodb(products: &std::collections::HashMap<String, Product>, client: &Client) -> mongodb::error::Result<()> {
    let database = client.database("skyblock");
    let collection: mongodb::Collection<Document> = database.collection("bazaar");

    let current_time = Utc::now();
    let bson_current_time = BsonDateTime::from_system_time(current_time.into());

    for product in products.values() {
        if product.buy_summary.is_empty() && product.sell_summary.is_empty() {
            continue;
        }

        let sell_summary_first3: Vec<ProductSummary> = product.sell_summary.iter().take(3).cloned().collect();
        let buy_summary_first3: Vec<ProductSummary> = product.buy_summary.iter().take(3).cloned().collect();

        let document = doc! {
            "product_id": &product.product_id,
            "sell_summary": bson::to_bson(&sell_summary_first3)?,
            "buy_summary": bson::to_bson(&buy_summary_first3)?,
            "quick_status": {
                "productId": &product.quick_status.productId,
                "sellPrice": product.quick_status.sellPrice,
                "sellVolume": product.quick_status.sellVolume,
                "sellMovingWeek": product.quick_status.sellMovingWeek,
                "sellOrders": product.quick_status.sellOrders,
                "buyPrice": product.quick_status.buyPrice,
                "buyVolume": product.quick_status.buyVolume,
                "buyMovingWeek": product.quick_status.buyMovingWeek,
                "buyOrders": product.quick_status.buyOrders,
            },
            "timestamp": bson_current_time,
        };

        collection.insert_one(document, None).await?;
    }

    Ok(())
}

async fn update_bazaarupdated(config_collection: &mongodb::Collection<Document>) -> Result<(), Box<dyn std::error::Error>> {
    config_collection.update_one(
        doc! { "bazaarupdated": { "$exists": true } },
        doc! { "$inc": { "bazaarupdated": 1 } },
        None,
    ).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    client_options.app_name = Some("hypixel".to_string());
    let client = Client::with_options(client_options)?;

    let mut last_updated: Option<i64> = None;

    loop {
        let bazaar_data = fetch_bazaar_data().await?;

        if Some(bazaar_data.lastUpdated) != last_updated {
            save_to_mongodb(&bazaar_data.products, &client).await?;
            last_updated = Some(bazaar_data.lastUpdated);
            let config_collection: mongodb::Collection<Document> = client.database("skyblock").collection("config");
            update_bazaarupdated(&config_collection).await?;
            println!("New data saved at {}", Utc::now());
        } else {
            println!("Data is up to date. No need to save.");
        }

        sleep(Duration::from_secs(5)).await;
    }
}
