use ordered_float::OrderedFloat;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

macro_rules! impl_from {
    ($target:ty; $($source:ty => |$param:ident| $body:expr),+ $(,)?) => {
        $(
            impl From<$source> for $target {
                #[inline]
                fn from($param: $source) -> Self {
                    $body
                }
            }
        )+
    };
}

macro_rules! impl_value_string_from {
    ($($source:ty),+ $(,)?) => {
        $(
            impl From<$source> for Value {
                #[inline]
                fn from(value: $source) -> Self {
                    Self::String(Str::from(value))
                }
            }
        )+
    };
}

macro_rules! impl_value_int_from {
    ($($source:ty),+ $(,)?) => {
        $(
            impl From<$source> for Value {
                #[inline]
                fn from(value: $source) -> Self {
                    Self::Int(i64::from(value))
                }
            }
        )+
    };
}

macro_rules! impl_value_double_from {
    ($($source:ty => $convert:expr),+ $(,)?) => {
        $(
            impl From<$source> for Value {
                #[inline]
                fn from(value: $source) -> Self {
                    Self::Double(OrderedFloat::from($convert(value)))
                }
            }
        )+
    };
}

#[derive(Debug, Clone)]
pub enum Str {
    Cow(Cow<'static, str>),
    ArcStr(Arc<str>),
}

impl_from!(Str;
    &'static str => |value| Self::Cow(Cow::Borrowed(value)),
    String => |value| Self::Cow(Cow::Owned(value)),
    Arc<str> => |value| Self::ArcStr(value),
    &Arc<str> => |value| Self::ArcStr(Arc::clone(value)),
    Cow<'static, str> => |value| Self::Cow(value),

    &String => |value| Self::Cow(Cow::Owned(value.clone())),
    &Str => |value| value.clone(),
    &Cow<'static, str> => |value| Self::Cow(value.clone()),
    Box<str> => |value| Self::Cow(Cow::Owned(value.into_string())),
);

impl Str {
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            Str::Cow(s) => s.as_ref(),
            Str::ArcStr(s) => s.as_ref(),
        }
    }

    #[inline]
    fn ptr_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ArcStr(l), Self::ArcStr(r)) => Arc::ptr_eq(l, r),

            (Self::Cow(Cow::Borrowed(l)), Self::Cow(Cow::Borrowed(r))) => {
                l.as_ptr() == r.as_ptr() && l.len() == r.len()
            }

            _ => false,
        }
    }
}

impl PartialEq for Str {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other) || self.as_str() == other.as_str()
    }
}

impl Display for Str {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Eq for Str {}

impl PartialOrd for Str {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Str {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        if self.ptr_eq(other) {
            Ordering::Equal
        } else {
            self.as_str().cmp(other.as_str())
        }
    }
}

impl Hash for Str {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Value {
    String(Str),
    Bool(bool),
    Int(i64),
    Double(OrderedFloat<f64>),
    ArrayAny(Vec<Value>),
    ArrayKv(Vec<KeyValue>),
    Bytes(Vec<u8>),
}

impl_value_string_from!(
    &'static str,
    String,
    &String,
    Arc<str>,
    &Arc<str>,
    Cow<'static, str>,
    &Cow<'static, str>,
    Box<str>,
    Str,
    &Str,
);

impl_from!(Value;
    bool => |value| Self::Bool(value),
    OrderedFloat<f64> => |value| Self::Double(value),

    Vec<Value> => |value| Self::ArrayAny(value),
    &[Value] => |value| Self::ArrayAny(value.to_vec()),

    Vec<KeyValue> => |value| Self::ArrayKv(value),
    &[KeyValue] => |value| Self::ArrayKv(value.to_vec()),

    Vec<u8> => |value| Self::Bytes(value),
    &[u8] => |value| Self::Bytes(value.to_vec()),
    Box<[u8]> => |value| Self::Bytes(value.into_vec()),
);

impl<const N: usize> From<[Value; N]> for Value {
    #[inline]
    fn from(value: [Value; N]) -> Self {
        Self::ArrayAny(Vec::from(value))
    }
}

impl<const N: usize> From<&[Value; N]> for Value {
    #[inline]
    fn from(value: &[Value; N]) -> Self {
        Self::ArrayAny(value.to_vec())
    }
}

impl<const N: usize> From<[KeyValue; N]> for Value {
    #[inline]
    fn from(value: [KeyValue; N]) -> Self {
        Self::ArrayKv(Vec::from(value))
    }
}

impl<const N: usize> From<&[KeyValue; N]> for Value {
    #[inline]
    fn from(value: &[KeyValue; N]) -> Self {
        Self::ArrayKv(value.to_vec())
    }
}

impl<const N: usize> From<[u8; N]> for Value {
    #[inline]
    fn from(value: [u8; N]) -> Self {
        Self::Bytes(Vec::from(value))
    }
}

impl<const N: usize> From<&[u8; N]> for Value {
    #[inline]
    fn from(value: &[u8; N]) -> Self {
        Self::Bytes(value.to_vec())
    }
}

impl_value_int_from!(i64, i8, i16, i32, u8, u16, u32,);

impl_value_double_from!(
    f64 => std::convert::identity,
    f32 => f64::from,
);

impl TryFrom<u64> for Value {
    type Error = std::num::TryFromIntError;

    #[inline]
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(Self::Int(i64::try_from(value)?))
    }
}

impl TryFrom<usize> for Value {
    type Error = std::num::TryFromIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self::Int(i64::try_from(value)?))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct KeyValue {
    pub key: Str,
    pub value: Option<Value>,
}

impl KeyValue {
    #[inline(always)]
    pub fn new(key: impl Into<Str>, value: impl Into<Value>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }

    #[inline(always)]
    pub fn no_val(key: impl Into<Str>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }
}

impl<K, V> From<(K, V)> for KeyValue
where
    K: Into<Str>,
    V: Into<Value>,
{
    #[inline]
    fn from((key, value): (K, V)) -> Self {
        Self::new(key, value)
    }
}

impl_from!(KeyValue;
    &KeyValue => |value| value.clone(),
    &&KeyValue => |value| (*value).clone(),
);

impl Display for KeyValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key)?;
        if let Some(value) = &self.value {
            write!(f, "={value}")?;
        }
        Ok(())
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => write!(f, "{:?}", value.as_str()),
            Self::Bool(value) => write!(f, "{value}"),
            Self::Int(value) => write!(f, "{value}"),
            Self::Double(value) => write!(f, "{}", value.0),
            Self::ArrayAny(values) => {
                write!(f, "[")?;
                for (idx, value) in values.iter().enumerate() {
                    if idx != 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{value}")?;
                }
                write!(f, "]")
            }
            Self::ArrayKv(values) => {
                write!(f, "{{")?;
                for (idx, value) in values.iter().enumerate() {
                    if idx != 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{value}")?;
                }
                write!(f, "}}")
            }
            Self::Bytes(values) => write!(f, "{:?}", values),
        }
    }
}

impl From<Str> for String {
    fn from(s: Str) -> Self {
        match s {
            Str::Cow(s) => match s {
                Cow::Borrowed(s) => s.to_owned(),
                Cow::Owned(s) => s,
            },
            Str::ArcStr(s) => s.as_ref().to_owned(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Temporality {
    Cumulative,
    Delta,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NameIdentity {
    pub name: Str,
    pub description: Str,
    pub unit: Str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api() {
        let _ = KeyValue::no_val("foo");
        let _ = KeyValue::new("foo", "bar");

        let s = "foobar".to_owned();
        let _ = KeyValue::new("a", &s);

        let _ = KeyValue::new("a", 12i32);
    }
}
