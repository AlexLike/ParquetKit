// Copyright 2026 Alexander Zank & contributors
// SPDX-License-Identifier: Apache-2.0
// This file is part of ParquetKit

use std::fs::File;
use std::sync::{Arc, Mutex};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::schema::arrow_to_schema;
use crate::value::extract_value;
use crate::{ColumnValue, FieldSchema, ParquetError};

#[derive(uniffi::Object)]
pub struct ReaderHandle {
  reader: Mutex<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + Send>>,
  current: Mutex<Option<(RecordBatch, usize)>>,
  schema: Vec<FieldSchema>,
}

#[uniffi::export]
impl ReaderHandle {
  #[uniffi::constructor]
  pub fn new(path: String) -> Result<Arc<Self>, ParquetError> {
    let file = File::open(&path).map_err(|e| ParquetError::Io {
      msg: format!("failed to open file '{path}': {e}"),
    })?;

    let builder =
      ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| ParquetError::InvalidFile {
        msg: format!("failed to read parquet file: {e}"),
      })?;

    let arrow_schema = builder.schema().clone();
    let schema = arrow_to_schema(&arrow_schema)?;

    // Read all columns (no projection).
    let mask = ProjectionMask::all();
    let reader = builder
      .with_projection(mask)
      .build()
      .map_err(|e| ParquetError::Io {
        msg: format!("failed to build reader: {e}"),
      })?;

    Ok(Arc::new(ReaderHandle {
      reader: Mutex::new(Box::new(reader)),
      current: Mutex::new(None),
      schema,
    }))
  }

  pub fn schema(&self) -> Result<Vec<FieldSchema>, ParquetError> {
    Ok(self.schema.clone())
  }

  pub fn read_row(&self) -> Result<Option<Vec<ColumnValue>>, ParquetError> {
    let mut current = self.current.lock().map_err(|e| ParquetError::Io {
      msg: format!("current lock poisoned: {e}"),
    })?;

    loop {
      if let Some((ref batch, ref mut row_idx)) = *current
        && *row_idx < batch.num_rows()
      {
        let row = *row_idx;
        *row_idx += 1;
        let mut values = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
          values.push(extract_value(col.as_ref(), row)?);
        }
        return Ok(Some(values));
      }

      // Need next batch.
      let mut reader = self.reader.lock().map_err(|e| ParquetError::Io {
        msg: format!("reader lock poisoned: {e}"),
      })?;

      match reader.next() {
        Some(Ok(batch)) => {
          *current = Some((batch, 0));
          // Loop back to extract from the new batch.
        }
        Some(Err(e)) => {
          return Err(ParquetError::Io {
            msg: format!("error reading batch: {e}"),
          });
        }
        None => {
          *current = None;
          return Ok(None);
        }
      }
    }
  }

  /// Opens the file projecting only the specified root-level columns.
  ///
  /// `columns` is a list of column names to include.  Names not present in
  /// the file are silently ignored.  The `schema()` of the returned handle
  /// reflects the projected columns only.
  #[uniffi::constructor]
  pub fn new_projected(path: String, columns: Vec<String>) -> Result<Arc<Self>, ParquetError> {
    let file = File::open(&path).map_err(|e| ParquetError::Io {
      msg: format!("failed to open file '{path}': {e}"),
    })?;

    let builder =
      ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| ParquetError::InvalidFile {
        msg: format!("failed to read parquet file: {e}"),
      })?;

    // Find root-level column indices for the requested column names.
    let indices: Vec<usize> = builder
      .parquet_schema()
      .root_schema()
      .get_fields()
      .iter()
      .enumerate()
      .filter_map(|(i, f)| {
        if columns.contains(&f.name().to_string()) {
          Some(i)
        } else {
          None
        }
      })
      .collect();

    let mask = ProjectionMask::roots(builder.parquet_schema(), indices);

    let batch_reader = builder
      .with_projection(mask)
      .build()
      .map_err(|e| ParquetError::Io {
        msg: format!("failed to build reader: {e}"),
      })?;

    let schema = arrow_to_schema(&batch_reader.schema())?;

    Ok(Arc::new(ReaderHandle {
      reader: Mutex::new(Box::new(batch_reader)),
      current: Mutex::new(None),
      schema,
    }))
  }

  pub fn close(&self) {
    // Drop the reader by replacing with an empty iterator.
    if let Ok(mut reader) = self.reader.lock() {
      *reader = Box::new(std::iter::empty());
    }
    if let Ok(mut current) = self.current.lock() {
      *current = None;
    }
  }
}
