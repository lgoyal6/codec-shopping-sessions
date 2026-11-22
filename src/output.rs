use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::analytics::{CampaignTotals, UserDay};
use crate::identity::{Campaigns, Products};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn string(value: &[u8]) -> Result<&str> {
    Ok(std::str::from_utf8(value)?)
}

fn writer(path: &Path, fields: Vec<Field>) -> Result<(ArrowWriter<File>, Arc<Schema>)> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_max_row_group_row_count(Some(65_536))
        .build();
    let schema = Arc::new(Schema::new(fields));
    let writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(properties))?;
    Ok((writer, schema))
}

fn write(
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
    columns: Vec<ArrayRef>,
) -> Result<()> {
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    writer.write(&batch)?;
    Ok(())
}

fn integer(value: u64) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| io::Error::other("aggregate exceeds signed 64-bit range"))
}

pub fn users<'a>(
    path: &Path,
    rows: impl Iterator<Item = &'a UserDay<'a>>,
    products: &Products<'_>,
) -> Result<()> {
    let (mut writer, schema) = writer(
        path,
        vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("day", DataType::Date32, false),
            Field::new("sessions", DataType::Int64, false),
            Field::new("events", DataType::Int64, false),
            Field::new("revenue_cents", DataType::Int64, false),
            Field::new("top_category", DataType::Utf8, true),
        ],
    )?;

    let mut rows = rows.peekable();
    while rows.peek().is_some() {
        let mut ids = StringBuilder::new();
        let mut days = Date32Builder::new();
        let mut sessions = Int64Builder::new();
        let mut events = Int64Builder::new();
        let mut revenue = Int64Builder::new();
        let mut category = StringBuilder::new();
        for row in rows.by_ref().take(65_536) {
            ids.append_value(string(row.person)?);
            days.append_value(row.day);
            sessions.append_value(integer(row.sessions)?);
            events.append_value(integer(row.events)?);
            revenue.append_value(integer(row.revenue)?);
            match row.category {
                Some(index) => category.append_value(string(products.categories[index as usize])?),
                None => category.append_null(),
            }
        }
        write(
            &mut writer,
            &schema,
            vec![
                Arc::new(ids.finish()),
                Arc::new(days.finish()),
                Arc::new(sessions.finish()),
                Arc::new(events.finish()),
                Arc::new(revenue.finish()),
                Arc::new(category.finish()),
            ],
        )?;
    }
    writer.close()?;
    Ok(())
}

pub fn campaigns(path: &Path, totals: CampaignTotals, dimensions: &Campaigns<'_>) -> Result<()> {
    let (mut writer, schema) = writer(
        path,
        vec![
            Field::new("campaign_id", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("day", DataType::Date32, false),
            Field::new("sessions", DataType::Int64, false),
            Field::new("purchasing_sessions", DataType::Int64, false),
            Field::new("revenue_cents", DataType::Int64, false),
            Field::new("assisted_sessions", DataType::Int64, false),
        ],
    )?;
    let mut ids = StringBuilder::new();
    let mut channels = StringBuilder::new();
    let mut days = Date32Builder::new();
    let mut sessions = Int64Builder::new();
    let mut purchases = Int64Builder::new();
    let mut revenue = Int64Builder::new();
    let mut assists = Int64Builder::new();
    for ((campaign, day), row) in totals {
        let [id, channel] = dimensions.names[campaign as usize];
        ids.append_value(string(id)?);
        channels.append_value(string(channel)?);
        days.append_value(day);
        sessions.append_value(integer(row.sessions)?);
        purchases.append_value(integer(row.purchases)?);
        revenue.append_value(integer(row.revenue)?);
        assists.append_value(integer(row.assists)?);
    }
    write(
        &mut writer,
        &schema,
        vec![
            Arc::new(ids.finish()),
            Arc::new(channels.finish()),
            Arc::new(days.finish()),
            Arc::new(sessions.finish()),
            Arc::new(purchases.finish()),
            Arc::new(revenue.finish()),
            Arc::new(assists.finish()),
        ],
    )?;
    writer.close()?;
    Ok(())
}
