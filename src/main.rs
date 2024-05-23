use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::DeltaOps;
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
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
            StructField::new(
                "half_amount",
                DataType::Primitive(PrimitiveType::Integer),
                false,
            ),
        ]
    }
}

async fn retriev_source_data(file_parquet: &str) -> Vec<RecordBatch> {
    let context = SessionContext::new();
    let field_id = Field::new("id", ArrowDataType::Int32, false);
    let field_name = Field::new("name", ArrowDataType::Utf8, false);
    let field_amount = Field::new("amount", ArrowDataType::Int32, false);

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
    let args: Vec<String> = env::args().collect();
    let source_number: i32 = match args.len() {
        1 => 1,
        _ => args[1].parse().unwrap(),
    };

    let file_parquet = format!("source_data/feed_{}.parquet", source_number);
    let table_path = "delta/source_table_bronze";

    let mut table = get_table(&table_path).await;

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut writer = RecordBatchWriter::for_table(&table)
        .expect("Failed to make RecordBatchWriter")
        .with_writer_properties(writer_properties);

    for batch in retriev_source_data(&file_parquet).await {
        writer.write(batch).await.unwrap();
    }

    let adds = writer
        .flush_and_commit(&mut table)
        .await
        .expect("Failed to flush write");
    info!("{} adds written", adds);
}
