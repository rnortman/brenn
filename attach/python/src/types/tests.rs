//! The refusal mappings, variant by variant: the Python member and every
//! value each carries. No interpreter takes part.

use super::*;

/// An upload refusal's member and its `(size, max, offset, len)`.
fn upload(refusal: UploadRefusal) -> (PyUploadRefusal, [Option<u64>; 4]) {
    let fields = upload_refusal(refusal);
    (
        fields.reason,
        [fields.size, fields.max, fields.offset, fields.len],
    )
}

#[test]
fn each_upload_refusal_maps_to_its_member_and_values() {
    for (refusal, expected) in [
        (
            UploadRefusal::BlobTooLarge { size: 7, max: 5 },
            (
                PyUploadRefusal::BlobTooLarge,
                [Some(7), Some(5), None, None],
            ),
        ),
        (
            UploadRefusal::BudgetExhausted,
            (PyUploadRefusal::BudgetExhausted, [None; 4]),
        ),
        (
            UploadRefusal::HandleLimit,
            (PyUploadRefusal::HandleLimit, [None; 4]),
        ),
        (
            UploadRefusal::BytesHeldLimit,
            (PyUploadRefusal::BytesHeldLimit, [None; 4]),
        ),
        (
            UploadRefusal::StorageFailed,
            (PyUploadRefusal::StorageFailed, [None; 4]),
        ),
        (
            UploadRefusal::Incomplete { offset: 3, len: 11 },
            (PyUploadRefusal::Incomplete, [None, None, Some(3), Some(11)]),
        ),
    ] {
        assert!(upload(refusal) == expected);
    }
}

#[test]
fn each_open_refusal_maps_to_its_member_and_size() {
    for (refusal, expected) in [
        (OpenRefusal::NotRetained, (PyOpenRefusal::NotRetained, None)),
        (
            OpenRefusal::NoSuchAttachment,
            (PyOpenRefusal::NoSuchAttachment, None),
        ),
        (OpenRefusal::ToolChannel, (PyOpenRefusal::ToolChannel, None)),
        (OpenRefusal::HandleLimit, (PyOpenRefusal::HandleLimit, None)),
        (
            OpenRefusal::BytesHeldLimit { size: 9 },
            (PyOpenRefusal::BytesHeldLimit, Some(9)),
        ),
    ] {
        assert!(open_refusal(refusal) == expected);
    }
}

#[test]
fn each_read_refusal_maps_to_its_member_and_size() {
    for (refusal, expected) in [
        (
            ReadRefusal::OutOfRange { size: 4 },
            (PyReadRefusal::OutOfRange, Some(4)),
        ),
        (
            ReadRefusal::UnknownHandle,
            (PyReadRefusal::UnknownHandle, None),
        ),
        (ReadRefusal::NotSealed, (PyReadRefusal::NotSealed, None)),
        (
            ReadRefusal::TooManyReads,
            (PyReadRefusal::TooManyReads, None),
        ),
        (
            ReadRefusal::StorageFailed,
            (PyReadRefusal::StorageFailed, None),
        ),
        (ReadRefusal::Closed, (PyReadRefusal::Closed, None)),
    ] {
        assert!(read_refusal(refusal) == expected);
    }
}
