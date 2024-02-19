use std::{cmp::Ordering, fmt::Debug};

#[derive(Debug, Clone)]
pub enum InfiniteKeys {
    Positive,
    Negative,
}
use InfiniteKeys::{Negative, Positive};

impl PartialEq<[u8]> for InfiniteKeys {
    fn eq(&self, _other: &[u8]) -> bool {
        false
    }
}

impl PartialEq<InfiniteKeys> for [u8] {
    fn eq(&self, _other: &InfiniteKeys) -> bool {
        false
    }
}

impl PartialOrd<[u8]> for InfiniteKeys {
    fn partial_cmp(&self, _other: &[u8]) -> Option<std::cmp::Ordering> {
        Some(match self {
            Positive => Ordering::Greater,
            Negative => Ordering::Less,
        })
    }
}

impl PartialOrd<InfiniteKeys> for [u8] {
    fn partial_cmp(&self, other: &InfiniteKeys) -> Option<Ordering> {
        Some(match other {
            Positive => Ordering::Less,
            Negative => Ordering::Greater,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_slice() {
        let a: Vec<u8> = vec![1, 2, 3];
        let b: &[u8] = &[5, 6, 7];
        assert!(InfiniteKeys::Positive > a[..]);
        assert!(InfiniteKeys::Positive > *b);
        assert!(a[..] < InfiniteKeys::Positive);
        assert!(*b < InfiniteKeys::Positive);
        assert!(a[..] >= InfiniteKeys::Negative);
        assert!(*b >= InfiniteKeys::Negative);
    }
}
