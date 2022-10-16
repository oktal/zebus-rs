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

#[derive(Copy, Clone, prost::Message)]
pub struct DateTime {
    #[prost(int64, tag = 1)]
    pub value: i64,

    #[prost(enumeration = "TimeSpanScale", tag = 2)]
    pub scale: i32,

    #[prost(enumeration = "DateTimeKind", tag = 3)]
    pub kind: i32,
}

impl From<chrono::DateTime<chrono::Utc>> for DateTime {
    fn from(dt: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            value: (dt.timestamp_nanos() / 100),
            scale: TimeSpanScale::Ticks as i32,
            kind: DateTimeKind::Utc as i32,
        }
    }
}
