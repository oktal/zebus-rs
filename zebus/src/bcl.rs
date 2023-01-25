use chrono::TimeZone;

use crate::proto::{prost, FromProtobuf, IntoProtobuf};

#[derive(Clone, Copy, Eq, PartialEq, prost::Message)]
pub struct Guid {
    #[prost(fixed64, tag = 1)]
    lo: u64,
    #[prost(fixed64, tag = 2)]
    hi: u64,
}

impl Guid {
    pub fn to_uuid(&self) -> uuid::Uuid {
        let (lo, hi) = (self.lo, self.hi);
        uuid::Uuid::from_u64_pair(lo, hi)
    }
}

impl From<uuid::Uuid> for Guid {
    fn from(uuid: uuid::Uuid) -> Self {
        let (lo, hi) = uuid.as_u64_pair();
        Self { hi, lo }
    }
}

impl FromProtobuf for uuid::Uuid {
    type Input = Guid;

    fn from_protobuf(input: Self::Input) -> Self {
        input.to_uuid()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, prost::Enumeration)]
#[repr(i32)]
pub enum TimeSpanScale {
    Days = 0,
    Hours = 1,
    Minutes = 2,
    Seconds = 3,
    Milliseconds = 4,
    Ticks = 5,
    MinMax = 15,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, prost::Enumeration)]
#[repr(i32)]
pub enum DateTimeKind {
    Unspecified = 0,
    Utc = 1,
    Local = 2,
}

#[derive(Copy, Clone, prost::Message)]
pub struct TimeSpan {
    #[prost(int64, tag = 1)]
    pub value: i64,

    #[prost(enumeration = "TimeSpanScale", tag = 2)]
    pub scale: i32,
}

pub enum DateTimeError {
    /// Attempted to convert a [`DateTime`] that was not in UTC
    NotUtc,

    /// Invalid timestamp
    InvalidTimestamp,
}

#[derive(Copy, Clone, prost::Message)]
pub struct DateTime {
    #[prost(int64, tag = 1)]
    pub value: i64,

    #[prost(enumeration = "TimeSpanScale", tag = 2)]
    pub scale: i32,

    #[prost(enumeration = "DateTimeKind", tag = 3)]
    pub kind: i32,
}

impl TryInto<chrono::DateTime<chrono::Utc>> for DateTime {
    type Error = DateTimeError;

    fn try_into(self) -> Result<chrono::DateTime<chrono::Utc>, Self::Error> {
        if self.kind() == DateTimeKind::Utc {
            // 100-ns ticks
            let timestamp_ns = self.value * 100;

            let (mut secs, mut nanos) =
                (timestamp_ns / 1_000_000_000, timestamp_ns % 1_000_000_000);
            if nanos < 0 {
                secs -= 1;
                nanos += 1_000_000_000;
            }

            chrono::Utc
                .timestamp_opt(secs, nanos as u32)
                .single()
                .ok_or(DateTimeError::InvalidTimestamp)
        } else {
            Err(DateTimeError::NotUtc)
        }
    }
}

impl IntoProtobuf for chrono::DateTime<chrono::Utc> {
    type Output = DateTime;

    fn into_protobuf(self) -> Self::Output {
        Self::Output {
            // 100-ns ticks
            value: (self.timestamp_nanos() / 100),
            scale: TimeSpanScale::Ticks as i32,
            kind: DateTimeKind::Utc as i32,
        }
    }
}
