use std::fs::File;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

use memchr::memchr;
use memmap2::{Mmap, MmapOptions};

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
const CHUNK_BYTES: usize = 1024 * 1024;

pub fn csv_paths(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in dir.read_dir()? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "csv") {
            paths.push(path);
        }
    }
    paths.sort_unstable();
    Ok(paths)
}

pub fn map_csv(path: &Path) -> io::Result<Option<Mmap>> {
    let file = File::open(path)?;
    if file.metadata()?.len() == 0 {
        return Ok(None);
    }
    // The scorer mounts the dataset read-only for the lifetime of the process.
    unsafe { MmapOptions::new().map(&file).map(Some) }
}

pub fn data_lines(bytes: &[u8]) -> &[u8] {
    memchr(b'\n', bytes).map_or(&[], |header| &bytes[header + 1..])
}

pub fn csv_chunks(files: &[Mmap]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    for file in files {
        let mut remaining = data_lines(file);
        while remaining.len() > CHUNK_BYTES {
            let Some(newline) = memchr(b'\n', &remaining[CHUNK_BYTES..]) else {
                break;
            };
            let split = CHUNK_BYTES + newline + 1;
            let (chunk, rest) = remaining.split_at(split);
            chunks.push(chunk);
            remaining = rest;
        }
        if !remaining.is_empty() {
            chunks.push(remaining);
        }
    }
    chunks
}

pub fn lines(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    let final_end = (!data.is_empty() && !data.ends_with(b"\n")).then_some(data.len());
    memchr::memchr_iter(b'\n', data)
        .chain(final_end)
        .map(move |end| {
            let line = &data[start..end];
            start = end + 1;
            line
        })
}

pub fn fields<const WIDTH: usize>(line: &[u8]) -> Option<[&[u8]; WIDTH]> {
    let mut values = [&[][..]; WIDTH];
    let (last, prefix) = values.split_last_mut()?;
    let mut commas = memchr::memchr_iter(b',', line);
    let mut start = 0;
    for value in prefix {
        let end = commas.next()?;
        *value = unquote(&line[start..end])?;
        start = end + 1;
    }
    if commas.next().is_some() {
        return None;
    }
    *last = unquote(&line[start..])?;
    Some(values)
}

#[inline]
fn unquote(mut value: &[u8]) -> Option<&[u8]> {
    if value.len() >= 2 && value[0] == b'"' && value[value.len() - 1] == b'"' {
        value = &value[1..value.len() - 1];
    }
    memchr(b'"', value).is_none().then_some(value)
}

#[inline]
fn two_digits(value: &[u8], at: usize) -> Option<i32> {
    let a = value[at].wrapping_sub(b'0');
    let b = value[at + 1].wrapping_sub(b'0');
    (a <= 9 && b <= 9).then_some(i32::from(a) * 10 + i32::from(b))
}

pub fn decimal(value: &[u8], maximum: i64) -> Option<i64> {
    if value.is_empty() {
        return None;
    }
    let mut total = 0_i64;
    for &byte in value {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        total = total.checked_mul(10)?.checked_add(i64::from(digit))?;
        if total > maximum {
            return None;
        }
    }
    Some(total)
}

pub fn timestamp(value: &[u8]) -> Option<i64> {
    if value.get(4) != Some(&b'-') {
        return decimal(value, i64::MAX);
    }
    let zone = match value.len() {
        20 if value[19] == b'Z' => 0,
        25 if matches!(value[19], b'+' | b'-') && value[22] == b':' => {
            let hours = two_digits(value, 20)?;
            let minutes = two_digits(value, 23)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let seconds = (hours * 60 + minutes) * 60;
            if value[19] == b'-' { -seconds } else { seconds }
        }
        _ => return None,
    };
    if value[7] != b'-' || value[10] != b'T' || value[13] != b':' || value[16] != b':' {
        return None;
    }
    let mut year = two_digits(value, 0)? * 100 + two_digits(value, 2)?;
    let month = two_digits(value, 5)?;
    let day = two_digits(value, 8)?;
    let hour = two_digits(value, 11)?;
    let minute = two_digits(value, 14)?;
    let second = two_digits(value, 17)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return None,
    };
    if year == 0 || day == 0 || day > month_days || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    // Count days in a March-based Gregorian year and subtract the Unix epoch.
    year -= i32::from(month <= 2);
    let era = year / 400;
    let year_of_era = year - era * 400;
    let march_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * march_month + 2) / 5 + day - 1;
    let days = era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100
        + day_of_year
        - 719_468;
    Some(i64::from(days) * DAY + i64::from((hour * 60 + minute) * 60 + second - zone) * 1000)
}

#[derive(Clone, Copy)]
pub struct Window {
    pub start: i64,
    pub end: i64,
}

impl Window {
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let manifest: serde_json::Value = serde_json::from_reader(File::open(path)?)?;
        let parse = |name: &str| {
            manifest
                .get(name)
                .and_then(serde_json::Value::as_str)
                .and_then(|s| timestamp(s.as_bytes()))
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, format!("invalid {name}")))
        };
        let start = parse("window_start")?;
        let end = parse("window_end")?;
        if start >= end || start.checked_sub(WEEK).is_none() {
            return Err(io::Error::new(ErrorKind::InvalidData, "invalid time window").into());
        }
        Ok(Self { start, end })
    }
}
