use std::{num::TryFromIntError, string::FromUtf8Error};
use thiserror::Error;

use hypercore::HypercoreError;
use prost::{DecodeError, EncodeError};

use crate::{
    blocks::BlocksBuilderError, traverse::TraverseConfigBuilderError, tree::TreeBuilderError,
    HyperbeeBuilderError,
};

#[derive(Error, Debug)]
pub enum HyperbeeError {
    #[error("There was an error in the underlying Hypercore")]
    HypercoreError(#[from] HypercoreError),
    #[error("There was an error decoding Hypercore data")]
    DecodeError(#[from] DecodeError),
    #[error("No block at seq  `{0}`")]
    NoBlockAtSeqError(u64),
    #[error("There was an error building `crate::Hyperbee` from `crate::tree::Tree`")]
    TreeBuilderError(#[from] TreeBuilderError),
    #[error(
        "There was an error building `crate::blocks::Blocks` from `crate::blocks::BlocksBuilder`"
    )]
    BlocksBuilderError(#[from] BlocksBuilderError),
    #[error("There was an error building `crate::Hyperbee` from `crate::HyperbeeBuilder`")]
    HyperbeeBuilderError(#[from] HyperbeeBuilderError),
    #[error("Converting a u64 value [{0}] to usize failed. This is possibly a 32bit platform. Got error {1}")]
    U64ToUsizeConversionError(u64, TryFromIntError),
    #[error("Could not traverse child node. Got error: {0}")]
    GetChildInTraverseError(Box<dyn std::error::Error>),
    #[error("There was an error building TraverseConfig")]
    TraverseConfigBuilderError(#[from] TraverseConfigBuilderError),
    #[error("There was an error building the iterator to traverse a node. Got error: {0}")]
    BuildIteratorInTraverseError(Box<dyn std::error::Error>),
    #[error("There was an error encoding a messages::YoloIndex {0}")]
    YoloIndexEncodingError(EncodeError),
    #[error("There was an error encoding a messages::Header {0}")]
    HeaderEncodingError(EncodeError),
    #[error("There was an error encoding a messages::Node {0}")]
    NodeEncodingError(EncodeError),
    #[error("There was an error decoding a key")]
    KeyFromUtf8Error(#[from] FromUtf8Error),
    #[error("The tree has no root so this operation failed")]
    NoRootError,
    #[error("The tree already has a header")]
    HeaderAlreadyExists,
}
