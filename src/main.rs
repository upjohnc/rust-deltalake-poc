use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::write::SchemaMode;
use deltalake::operations::DeltaOps;
use deltalake::{
    arrow::array::RecordBatch,
    datafusion::{
        dataframe::DataFrame,
        execution::{context::SessionContext, options::ParquetReadOptions},
        logical_expr::{cast, col, lit},
    },
};
use deltalake::{DeltaTable, DeltaTableError};
use log::info;
use std::env;

async fn create_table(path: &str) -> DeltaTable {
    DeltaOps::try_from_uri(path)
        .await
        .unwrap()
        .create()
        .with_columns(CustomerAmount::columns())
        .await
        .unwrap()
}

struct CustomerAmount {}

impl CustomerAmount {
    fn columns() -> Vec<StructField> {
        vec![
            StructField::new("id", DataType::Primitive(PrimitiveType::Integer), false),
            StructField::new("name", DataType::Primitive(PrimitiveType::String), false),
            StructField::new("amount", DataType::Primitive(PrimitiveType::Integer), false),
        ]
    }
}

async fn retrieve_source_data(file_parquet: &str) -> Vec<RecordBatch> {
    let context = SessionContext::new();
    let field_id = Field::new("id", ArrowDataType::Int32, true);
    let field_name = Field::new("name", ArrowDataType::Utf8, true);
    let field_amount = Field::new("amount", ArrowDataType::Int32, true);

    let schema = Schema::new(vec![field_id, field_name, field_amount]);

    let mut read_options = ParquetReadOptions::default();
    read_options.schema = Some(&schema);

    let df: DataFrame = context
        .read_parquet(file_parquet, read_options)
        .await
        .unwrap();

    let df = df
        .with_column(
            "half_amount",
            cast(col("amount") / lit(2), ArrowDataType::Int32),
        )
        .unwrap();

    let df_batches: Vec<RecordBatch> = df.collect().await.unwrap();
    df_batches
}

async fn get_table(table_path: &str) -> DeltaTable {
    let maybe_table = deltalake::open_table(&table_path).await;
    let table = match maybe_table {
        Ok(table) => table,
        Err(DeltaTableError::InvalidTableLocation(_)) | Err(DeltaTableError::NotATable(_)) => {
            info!("Creating table");
            create_table(&table_path).await
        }
        Err(err) => panic!("{:?}", err),
    };
    table
}

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let args: Vec<String> = env::args().collect();
    let source_number: i32 = match args.len() {
        1 => 1,
        _ => args[1].parse().unwrap(),
    };

    let file_parquet = format!("source_data/feed_{}.parquet", source_number);
    let table_path = "delta/source_table_bronze";

    let table = get_table(&table_path).await;

    let table_write = DeltaOps(table.clone())
        .write(retrieve_source_data(&file_parquet).await)
        .with_schema_mode(SchemaMode::Merge)
        .await
        .unwrap();
    let table = get_table(&table_path).await;

    match deltalake::checkpoints::create_checkpoint(&table).await {
        Ok(_) => info!("Successfully created checkpoint"),
        Err(e) => {
            println!("Failed to create checkpoint for {table_path}: {e:?}")
        }
    }

    info!("Table Save: {}", table_write);
}
