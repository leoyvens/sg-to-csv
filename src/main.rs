use anyhow::Error;
use async_compression::futures::write::GzipEncoder;
use clap::Parser;
use futures_util::{AsyncWrite, AsyncWriteExt, StreamExt};
use opendal::{services::Fs, Operator};

/// SG to CSV
#[derive(Parser, Debug)]
struct Args {
    /// Database URL
    #[clap(long, env = "SG_TO_CSV_DATABASE_URL")]
    database_url: String,

    /// Out directory
    #[clap(short, long, env = "SG_TO_CSV_OUT_DIR", default_value = "./")]
    out_dir: String,

    /// Schema name to process
    #[clap(validator = validate_schema)]
    schema: String,

    /// Turns off gzip compression
    #[clap(long)]
    no_compression: bool,
}

// Custom validator for schema name
fn validate_schema(val: &str) -> Result<(), String> {
    if val.chars().all(char::is_alphanumeric) {
        Ok(())
    } else {
        Err(String::from("Schema name must be alphanumeric"))
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments
    let args = Args::parse();

    // Load environment and CLI variables
    let database_url = args.database_url;
    let schema_name = args.schema;
    let out_dir = args.out_dir;
    let no_compression = args.no_compression;

    // Connect to the database
    let (client, conn) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls).await?;

    // Monitor for connection errors
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Filesystem writer
    let fs = {
        let mut fs = Fs::default();
        fs.root(&out_dir);
        Operator::new(fs)?.finish()
    };

    // Get the list of tables.
    // Security: `schema_name` is validated to be alphanumeric.
    let tables_query = format!(
        "SELECT tablename FROM pg_tables WHERE schemaname = '{}'",
        schema_name
    );
    let tables = client.query(&tables_query, &[]).await?;

    // Dump tables. Parallelizing this would involve using multiple DB connections.
    for table in tables {
        let table_name = table.get::<_, &str>(0).to_owned();

        dump_table_contents(&client, &schema_name, table_name, &fs, no_compression).await?;
    }

    Ok(())
}

async fn dump_table_contents(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: String,
    fs: &Operator,
    no_compression: bool,
) -> Result<(), Error> {
    let ext = if no_compression { "csv" } else { "csv.gz" };
    let path = format!("{}_{}.{}", schema_name, table_name, ext);

    let file_writer = fs.writer(&path).await?;

    let mut writer: Box<dyn AsyncWrite + Unpin> = if no_compression {
        Box::new(file_writer)
    } else {
        Box::new(GzipEncoder::new(file_writer))
    };

    // Security: `schema_name` is validated to be alphanumeric, `table_name` is from the database.
    let mut copy_stmt_stream = {
        let copy_query = format!(
            "COPY (select * from {}.{} order by vid) TO STDOUT WITH (FORMAT CSV, HEADER)",
            schema_name, table_name
        );
        let stmt = client.prepare(&copy_query).await?;
        Box::pin(client.copy_out(&stmt).await?)
    };

    // Copy the data into the file
    while let Some(row) = copy_stmt_stream.next().await {
        let row = row?;
        writer.write_all(&row).await?;
    }

    Ok(())
}
