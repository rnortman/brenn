//! A Gmail attachment fetched by its part: the `fields` mask that lists a
//! message's part tree to the fetch depth, and a push decoder that turns
//! `messages.attachments.get`'s JSON — `{"attachmentId", "size", "data"}`,
//! `data` base64url — into the attachment's bytes, piece by piece, with no
//! I/O and nothing held but the decoder's state.

use super::ATTACHMENT_PART_DEPTH;
use super::decode::Base64Url;

/// What one part of the tree lists: enough to find a part by `partId` and
/// fetch it, and no `data`.
const PART_FIELDS: &str = "partId,mimeType,filename,body(size,attachmentId)";

/// The `fields` mask of a `messages.get` that lists the payload and
/// [`ATTACHMENT_PART_DEPTH`] levels of `parts` below it, each part's
/// [`PART_FIELDS`] and none of its `data`.
pub fn part_tree_fields() -> String {
    super::part_tree_mask(PART_FIELDS, ATTACHMENT_PART_DEPTH)
}

/// Why an attachment's JSON could not be decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Not one JSON object of Gmail's shape: malformed JSON, a truncated
    /// object, bytes after it, no `data` or a second one, a `data` that is
    /// not a string, or a `size` that is not a non-negative integer or comes
    /// twice.
    Shape,
    /// `data` is not base64url: a character outside either alphabet, a
    /// backslash, or a final group of one character.
    Base64,
}

/// What a whole input decoded to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Finished {
    /// The decoded bytes' count.
    pub decoded: u64,
    /// The object's `size`, when it has one.
    pub size: Option<u64>,
}

/// The top-level keys the decoder reads; every other key's value is skipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Key {
    Data,
    Size,
    Other,
}

/// The longest key compared against `data` and `size`; a longer one is
/// another key.
const KEY_MAX_BYTES: usize = 4;

/// An escape being read inside a string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Escape {
    None,
    /// After `\`.
    Backslash,
    /// After `\u` and this many hex digits, holding their value.
    Unicode {
        digits: u8,
        value: u32,
    },
}

/// A number being read, by the JSON grammar's positions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Number {
    Minus,
    Zero,
    Int,
    Dot,
    Frac,
    Exp,
    ExpSign,
    ExpDigits,
}

impl Number {
    /// The position after `b`, or `None` when `b` does not continue the
    /// number.
    fn next(self, b: u8) -> Option<Number> {
        use Number::*;
        Some(match (self, b) {
            (Minus, b'0') => Zero,
            (Minus, b'1'..=b'9') => Int,
            (Int, b'0'..=b'9') => Int,
            (Zero | Int, b'.') => Dot,
            (Dot | Frac, b'0'..=b'9') => Frac,
            (Zero | Int | Frac, b'e' | b'E') => Exp,
            (Exp, b'+' | b'-') => ExpSign,
            (Exp | ExpSign | ExpDigits, b'0'..=b'9') => ExpDigits,
            _ => return None,
        })
    }

    /// Whether a number may end here.
    fn complete(self) -> bool {
        matches!(
            self,
            Number::Zero | Number::Int | Number::Frac | Number::ExpDigits
        )
    }
}

/// Where in the input the decoder stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// Before the top-level `{`.
    Start,
    /// After a `{`: a key or `}`.
    ObjectOpen,
    /// After a `,` in an object: a key.
    Key,
    /// After a key: `:`.
    Colon,
    /// A value.
    Value,
    /// After a `[`: a value or `]`.
    ArrayOpen,
    /// After a value inside a container: `,` or its close.
    AfterValue,
    /// Inside a string, a key when `key`.
    Str { key: bool, escape: Escape },
    /// Inside `true`, `false` or `null`, this many bytes in.
    Literal { word: &'static [u8], at: u8 },
    /// Inside a number that is skipped.
    Number(Number),
    /// Inside `data`'s string.
    Data,
    /// Inside `size`'s digits; `zero` when the first digit was `0`.
    Size { zero: bool },
    /// After the top-level object's `}`.
    End,
    /// An error was answered; it is answered again.
    Failed(DecodeError),
}

/// A container open around the current position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Container {
    Object,
    Array,
}

/// A push decoder of one `messages.attachments.get` answer: the JSON fed in
/// pieces split anywhere, `data`'s bytes produced as they decode.
///
/// It reads one top-level JSON object in any key order. `data`'s string is
/// decoded by [`Base64Url`], the decoder behind `decode::base64url`; `size`
/// is read as a non-negative integer; every other key's value is skipped
/// whatever its type. After an error every later call answers the same error.
#[derive(Debug)]
pub struct Decoder {
    state: State,
    /// The containers open, the top-level object first.
    open: Vec<Container>,
    /// The key being read at the top level, while it can still be `data` or
    /// `size`.
    key: [u8; KEY_MAX_BYTES],
    key_len: usize,
    /// The key read can no longer be `data` or `size`.
    key_other: bool,
    /// The top-level key whose value comes next.
    pending: Key,
    seen_data: bool,
    size: Option<u64>,
    /// `size`'s digits so far.
    size_value: u64,
    /// `data`'s decoding so far.
    base64: Base64Url,
    decoded: u64,
}

impl Default for Decoder {
    fn default() -> Decoder {
        Decoder::new()
    }
}

impl Decoder {
    pub fn new() -> Decoder {
        Decoder {
            state: State::Start,
            open: Vec::new(),
            key: [0; KEY_MAX_BYTES],
            key_len: 0,
            key_other: false,
            pending: Key::Other,
            seen_data: false,
            size: None,
            size_value: 0,
            base64: Base64Url::default(),
            decoded: 0,
        }
    }

    /// Feed the next piece of the JSON; decoded bytes are appended to `out`.
    pub fn push(&mut self, piece: &[u8], out: &mut Vec<u8>) -> Result<(), DecodeError> {
        if let State::Failed(error) = self.state {
            return Err(error);
        }
        match self.feed(piece, out) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.state = State::Failed(error);
                Err(error)
            }
        }
    }

    /// End of input: the decoded length and the `size` the object stated.
    pub fn finish(self) -> Result<Finished, DecodeError> {
        match self.state {
            State::Failed(error) => Err(error),
            State::End if self.seen_data => Ok(Finished {
                decoded: self.decoded,
                size: self.size,
            }),
            _ => Err(DecodeError::Shape),
        }
    }

    fn feed(&mut self, piece: &[u8], out: &mut Vec<u8>) -> Result<(), DecodeError> {
        use DecodeError::Shape;
        let mut i = 0;
        while i < piece.len() {
            let b = piece[i];
            match self.state {
                State::Data => {
                    i = self.data(piece, i, out)?;
                    continue;
                }
                State::Start => match b {
                    _ if is_space(b) => {}
                    b'{' => {
                        self.open.push(Container::Object);
                        self.state = State::ObjectOpen;
                    }
                    _ => return Err(Shape),
                },
                State::ObjectOpen | State::Key => match b {
                    _ if is_space(b) => {}
                    b'"' => {
                        self.key_len = 0;
                        self.key_other = false;
                        self.state = State::Str {
                            key: true,
                            escape: Escape::None,
                        };
                    }
                    b'}' if self.state == State::ObjectOpen => self.close(Container::Object)?,
                    _ => return Err(Shape),
                },
                State::Colon => match b {
                    _ if is_space(b) => {}
                    b':' => self.state = State::Value,
                    _ => return Err(Shape),
                },
                State::ArrayOpen if b == b']' => self.close(Container::Array)?,
                State::Value | State::ArrayOpen => {
                    if !is_space(b) {
                        self.value(b)?;
                    }
                }
                State::AfterValue => match b {
                    _ if is_space(b) => {}
                    b',' => {
                        self.state = match self.open.last() {
                            Some(Container::Object) => State::Key,
                            Some(Container::Array) => State::Value,
                            None => unreachable!("a value inside no container"),
                        }
                    }
                    b'}' => self.close(Container::Object)?,
                    b']' => self.close(Container::Array)?,
                    _ => return Err(Shape),
                },
                State::Str { key, escape } => self.string(key, escape, b)?,
                State::Literal { word, at } => {
                    if word[usize::from(at)] != b {
                        return Err(Shape);
                    }
                    let at = at + 1;
                    self.state = if usize::from(at) == word.len() {
                        State::AfterValue
                    } else {
                        State::Literal { word, at }
                    };
                }
                State::Number(number) => match number.next(b) {
                    Some(next) => self.state = State::Number(next),
                    None if number.complete() => {
                        // The byte after the number is read again as what
                        // follows a value.
                        self.state = State::AfterValue;
                        continue;
                    }
                    None => return Err(Shape),
                },
                State::Size { zero } => match b {
                    b'0'..=b'9' if !zero => {
                        self.size_value = self
                            .size_value
                            .checked_mul(10)
                            .and_then(|v| v.checked_add(u64::from(b - b'0')))
                            .ok_or(Shape)?;
                    }
                    b'0'..=b'9' | b'.' | b'e' | b'E' => return Err(Shape),
                    _ => {
                        self.size = Some(self.size_value);
                        self.state = State::AfterValue;
                        continue;
                    }
                },
                State::End => {
                    if !is_space(b) {
                        return Err(Shape);
                    }
                }
                State::Failed(error) => return Err(error),
            }
            i += 1;
        }
        Ok(())
    }

    /// `data`'s string from `piece[from..]`: the index after the last byte
    /// read.
    fn data(&mut self, piece: &[u8], from: usize, out: &mut Vec<u8>) -> Result<usize, DecodeError> {
        let before = out.len();
        let stop = from + self.base64.push(&piece[from..], out);
        self.decoded += (out.len() - before) as u64;
        match piece.get(stop) {
            None => Ok(stop),
            Some(b'"') if self.base64.whole() => {
                self.state = State::AfterValue;
                Ok(stop + 1)
            }
            Some(_) => Err(DecodeError::Base64),
        }
    }

    /// The first byte `b` of a value.
    fn value(&mut self, b: u8) -> Result<(), DecodeError> {
        use DecodeError::Shape;
        let top = self.open.len() == 1;
        let pending = if top { self.pending } else { Key::Other };
        self.state = match (pending, b) {
            (Key::Data, b'"') => {
                self.seen_data = true;
                State::Data
            }
            (Key::Data, _) => return Err(Shape),
            (Key::Size, b'0'..=b'9') => {
                self.size_value = u64::from(b - b'0');
                State::Size { zero: b == b'0' }
            }
            (Key::Size, _) => return Err(Shape),
            (Key::Other, b'"') => State::Str {
                key: false,
                escape: Escape::None,
            },
            (Key::Other, b'{') => {
                self.open.push(Container::Object);
                State::ObjectOpen
            }
            (Key::Other, b'[') => {
                self.open.push(Container::Array);
                State::ArrayOpen
            }
            (Key::Other, b't') => State::Literal {
                word: b"true",
                at: 1,
            },
            (Key::Other, b'f') => State::Literal {
                word: b"false",
                at: 1,
            },
            (Key::Other, b'n') => State::Literal {
                word: b"null",
                at: 1,
            },
            (Key::Other, b'-') => State::Number(Number::Minus),
            (Key::Other, b'0') => State::Number(Number::Zero),
            (Key::Other, b'1'..=b'9') => State::Number(Number::Int),
            (Key::Other, _) => return Err(Shape),
        };
        Ok(())
    }

    /// One byte `b` of a string, a top-level key when `key` and the object
    /// is the top level.
    fn string(&mut self, key: bool, escape: Escape, b: u8) -> Result<(), DecodeError> {
        use DecodeError::Shape;
        let capture = key && self.open.len() == 1;
        let next = match escape {
            Escape::None => match b {
                b'"' => {
                    self.state = if key {
                        if capture {
                            self.pending = self.read_key()?;
                        }
                        State::Colon
                    } else {
                        State::AfterValue
                    };
                    return Ok(());
                }
                b'\\' => Escape::Backslash,
                0x00..=0x1f => return Err(Shape),
                _ => {
                    if capture {
                        self.key_byte(b);
                    }
                    Escape::None
                }
            },
            Escape::Backslash => match b {
                b'u' => Escape::Unicode {
                    digits: 0,
                    value: 0,
                },
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                    if capture {
                        self.key_byte(unescaped(b));
                    }
                    Escape::None
                }
                _ => return Err(Shape),
            },
            Escape::Unicode { digits, value } => {
                let digit = char::from(b).to_digit(16).ok_or(Shape)?;
                let value = (value << 4) | digit;
                if digits < 3 {
                    Escape::Unicode {
                        digits: digits + 1,
                        value,
                    }
                } else {
                    if capture {
                        match u8::try_from(value) {
                            Ok(ascii) if ascii.is_ascii() => self.key_byte(ascii),
                            _ => self.key_other = true,
                        }
                    }
                    Escape::None
                }
            }
        };
        self.state = State::Str { key, escape: next };
        Ok(())
    }

    fn key_byte(&mut self, b: u8) {
        if self.key_len == KEY_MAX_BYTES {
            self.key_other = true;
        } else {
            self.key[self.key_len] = b;
            self.key_len += 1;
        }
    }

    /// The top-level key just read, refusing a second `data` or `size`.
    fn read_key(&self) -> Result<Key, DecodeError> {
        if self.key_other {
            return Ok(Key::Other);
        }
        Ok(match &self.key[..self.key_len] {
            b"data" if self.seen_data => return Err(DecodeError::Shape),
            b"data" => Key::Data,
            b"size" if self.size.is_some() => return Err(DecodeError::Shape),
            b"size" => Key::Size,
            _ => Key::Other,
        })
    }

    /// The close of `container`, which must be the innermost open.
    fn close(&mut self, container: Container) -> Result<(), DecodeError> {
        if self.open.pop() != Some(container) {
            return Err(DecodeError::Shape);
        }
        if self.open.len() == 1 {
            self.pending = Key::Other;
        }
        self.state = if self.open.is_empty() {
            State::End
        } else {
            State::AfterValue
        };
        Ok(())
    }
}

fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

/// The byte a one-character escape stands for.
fn unescaped(b: u8) -> u8 {
    match b {
        b'b' => 0x08,
        b'f' => 0x0c,
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `input` fed in `pieces` (split at each index given), and the decoded
    /// bytes with the result of `finish`.
    fn run(input: &[u8], splits: &[usize]) -> (Vec<u8>, Result<Finished, DecodeError>) {
        let mut decoder = Decoder::new();
        let mut out = Vec::new();
        let mut from = 0;
        for &at in splits.iter().chain([input.len()].iter()) {
            if let Err(error) = decoder.push(&input[from..at], &mut out) {
                return (out, Err(error));
            }
            from = at;
        }
        (out, decoder.finish())
    }

    fn whole(input: &str) -> (Vec<u8>, Result<Finished, DecodeError>) {
        run(input.as_bytes(), &[])
    }

    fn decoded(input: &str) -> Vec<u8> {
        let (out, finished) = whole(input);
        let finished = finished.unwrap_or_else(|e| panic!("{input}: {e:?}"));
        assert_eq!(finished.decoded, out.len() as u64);
        out
    }

    #[test]
    fn the_mask_nests_the_fetch_depth_below_the_payload() {
        let mask = part_tree_fields();
        assert_eq!(mask.matches("parts(").count(), ATTACHMENT_PART_DEPTH);
        assert_eq!(mask.matches("partId").count(), ATTACHMENT_PART_DEPTH + 1);
        assert!(!mask.contains("data"));
        assert!(
            mask.starts_with("payload(partId,mimeType,filename,body(size,attachmentId),parts(")
        );
        assert_eq!(mask.matches('(').count(), mask.matches(')').count());
        assert!(mask.ends_with(&")".repeat(ATTACHMENT_PART_DEPTH + 2)));
    }

    #[test]
    fn data_and_size_are_read_in_either_key_order() {
        // "hello"
        for input in [
            r#"{"size": 5, "data": "aGVsbG8"}"#,
            r#"{"data":"aGVsbG8","size":5}"#,
            r#"  {"attachmentId":"A-1" ,"data" : "aGVsbG8" , "size" : 5 }  "#,
        ] {
            let (out, finished) = whole(input);
            assert_eq!(out, b"hello", "{input}");
            assert_eq!(
                finished,
                Ok(Finished {
                    decoded: 5,
                    size: Some(5)
                })
            );
        }
        let (_, finished) = whole(r#"{"data":"aGVsbG8"}"#);
        assert_eq!(finished.unwrap().size, None);
    }

    #[test]
    fn every_other_key_is_skipped_whatever_its_type() {
        let input = r#"{"s":"a \"q\" \\ \u00e9 data","n":-1.5e+3,"z":0,"i":12,"f":0.25E-2,
            "t":true,"F":false,"u":null,"o":{"data":[1,{"size":"x"}],"k":{}},"a":[],
            "aa":[[],[null,"x",{"a":1}]],"data":"aGk","dat":"x","datas":1,"Data":2}"#;
        assert_eq!(decoded(input), b"hi");
    }

    #[test]
    fn an_escaped_key_is_the_key_it_spells() {
        assert_eq!(decoded(r#"{"d\u0061ta":"aGk"}"#), b"hi");
        assert_eq!(
            whole(r#"{"data":"aGk","\u0064ata":"aGk"}"#).1,
            Err(DecodeError::Shape)
        );
        // A key with a non-ASCII escape is another key.
        assert_eq!(decoded(r#"{"dat\u00e1":"x","data":"aGk"}"#), b"hi");
    }

    #[test]
    fn both_alphabets_padding_and_whitespace_decode() {
        // 0xfb 0xff 0xbf, which spells `+/+/` and `-_-_`.
        assert_eq!(decoded(r#"{"data":"-_-_"}"#), [0xfb, 0xff, 0xbf]);
        assert_eq!(decoded(r#"{"data":"+/+/"}"#), [0xfb, 0xff, 0xbf]);
        assert_eq!(decoded(r#"{"data":"aGk="}"#), b"hi");
        assert_eq!(decoded(r#"{"data":"aA=="}"#), b"h");
        // Raw whitespace inside `data`, as `decode::base64url` skips it.
        assert_eq!(decoded("{\"data\":\"aG Vs\r\nbG8\t\"}"), b"hello");
        assert_eq!(decoded(r#"{"data":""}"#), b"");
    }

    #[test]
    fn data_that_is_not_base64_is_refused() {
        for input in [
            r#"{"data":"aGk*"}"#,
            r#"{"data":"aG\u006b"}"#,
            r#"{"data":"aG\/k"}"#,
            r#"{"data":"aGVsb"}"#,
            "{\"data\":\"a\u{e9}\"}",
        ] {
            assert_eq!(whole(input).1, Err(DecodeError::Base64), "{input}");
        }
    }

    #[test]
    fn what_is_not_one_object_of_gmails_shape_is_refused() {
        for input in [
            "",
            "   ",
            "[]",
            r#"{"data":"aGk"} x"#,
            r#"{"data":"aGk"}{}"#,
            r#"{"data":"aGk""#,
            r#"{"data":"aGk"#,
            r#"{"data":"aGk","#,
            r#"{"data":"aGk",}"#,
            r#"{"size":1}"#,
            r#"{}"#,
            r#"{"data":"aGk","data":"aGk"}"#,
            r#"{"data":"aGk","size":1,"size":1}"#,
            r#"{"data":1}"#,
            r#"{"data":null}"#,
            r#"{"data":"aGk","size":-1}"#,
            r#"{"data":"aGk","size":1.0}"#,
            r#"{"data":"aGk","size":1e3}"#,
            r#"{"data":"aGk","size":01}"#,
            r#"{"data":"aGk","size":"1"}"#,
            r#"{"data":"aGk","size":99999999999999999999}"#,
            r#"{"data":"aGk","x":tru}"#,
            r#"{"data":"aGk","x":01}"#,
            r#"{"data":"aGk","x":1.}"#,
            r#"{"data":"aGk","x":-}"#,
            r#"{"data":"aGk","x":[1,]}"#,
            r#"{"data":"aGk","x":[1}"#,
            r#"{"data":"aGk","x":{"a"}}"#,
            r#"{"data":"aGk","x":"\q"}"#,
            r#"{"data":"aGk","x":"\u12g4"}"#,
            "{\"data\":\"aGk\",\"x\":\"a\nb\"}",
            r#"{"data":"aGk" "x":1}"#,
            r#"{"data" "aGk"}"#,
        ] {
            assert_eq!(whole(input).1, Err(DecodeError::Shape), "{input}");
        }
    }

    #[test]
    fn an_error_is_answered_again() {
        let mut decoder = Decoder::new();
        let mut out = Vec::new();
        assert_eq!(
            decoder.push(b"{\"data\":\"*", &mut out),
            Err(DecodeError::Base64)
        );
        assert_eq!(decoder.push(b"\"}", &mut out), Err(DecodeError::Base64));
        assert_eq!(decoder.finish(), Err(DecodeError::Base64));
    }

    /// A small input exercising every kind of token, and what it decodes to.
    const SMALL: &str = r#" {"attachmentId":"A\u002d1","meta":{"n":[1,-2.5e3,true,null],"s":"\"x\""},
        "size" : 10, "data":"aGVs bG8g-_-_IQ=="} "#;
    const SMALL_BYTES: &[u8] = b"hello \xfb\xff\xbf!";

    #[test]
    fn every_split_into_two_pieces_decodes_the_same() {
        let input = SMALL.as_bytes();
        for at in 0..=input.len() {
            let (out, finished) = run(input, &[at]);
            assert_eq!(out, SMALL_BYTES, "split at {at}");
            assert_eq!(
                finished,
                Ok(Finished {
                    decoded: 10,
                    size: Some(10)
                }),
                "split at {at}"
            );
        }
    }

    #[test]
    fn one_byte_pieces_decode_the_same() {
        let input = SMALL.as_bytes();
        let splits: Vec<usize> = (1..input.len()).collect();
        let (out, finished) = run(input, &splits);
        assert_eq!(out, SMALL_BYTES);
        assert_eq!(finished.unwrap().size, Some(10));
    }

    #[test]
    fn a_large_attachment_decodes_piece_by_piece() {
        let bytes: Vec<u8> = (0..300_000u32).map(|i| (i * 7 + i / 251) as u8).collect();
        let encoded = crate::testutil::base64url(&bytes);
        let input = format!(r#"{{"size":{},"data":"{encoded}"}}"#, bytes.len());
        let input = input.as_bytes();
        let splits: Vec<usize> = (1..input.len() / 1021).map(|n| n * 1021).collect();
        let (out, finished) = run(input, &splits);
        assert_eq!(out, bytes);
        assert_eq!(finished.unwrap().decoded, bytes.len() as u64);
    }
}
