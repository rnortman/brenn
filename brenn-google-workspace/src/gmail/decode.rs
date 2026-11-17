//! Turning Gmail's encodings into text a model reads: a header's value from
//! `payload.headers`, HTML character references in `snippet`, `internalDate`
//! as an RFC 3339 instant, and a `format=full` message's MIME tree as its text
//! parts, attachments and decoding notes — base64url, three charsets and HTML
//! read as text here. The tree is read into [`Part`], which keeps only what
//! the walk reads.

use std::borrow::Cow;
use std::fmt;
use std::ops::Range;

use serde::de::{self, Deserialize, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::Value;

/// `headers` is not Gmail's header list: not an array, or an entry in it
/// without a string `name` and a string `value`.
#[derive(Debug, PartialEq, Eq)]
pub struct MalformedHeaders;

/// The value of the first header in `headers` — Gmail's `[{ "name", "value" }]`
/// list — whose name is `name`, compared case-insensitively; `None` when no
/// entry has that name.
pub fn header<'a>(headers: &'a Value, name: &str) -> Result<Option<&'a str>, MalformedHeaders> {
    let entries = headers.as_array().ok_or(MalformedHeaders)?;
    let mut found = None;
    for entry in entries {
        let (Some(Value::String(n)), Some(Value::String(value))) =
            (entry.get("name"), entry.get("value"))
        else {
            return Err(MalformedHeaders);
        };
        if found.is_none() && n.eq_ignore_ascii_case(name) {
            found = Some(value.as_str());
        }
    }
    Ok(found)
}

/// Named references decoded, each to its text: the XML five, spaces, dashes
/// and quotes, symbols and currency, and the accented letters mail most
/// often spells this way. Names are case-sensitive, as HTML's are. An empty
/// text drops the reference: a soft hyphen or a zero-width joiner carries
/// nothing a reader needs.
const NAMED: &[(&str, &str)] = &[
    ("amp", "&"),
    ("lt", "<"),
    ("gt", ">"),
    ("quot", "\""),
    ("apos", "'"),
    ("nbsp", "\u{a0}"),
    ("ensp", "\u{2002}"),
    ("emsp", "\u{2003}"),
    ("thinsp", "\u{2009}"),
    ("shy", ""),
    ("zwj", ""),
    ("zwnj", ""),
    ("ndash", "\u{2013}"),
    ("mdash", "\u{2014}"),
    ("lsquo", "\u{2018}"),
    ("rsquo", "\u{2019}"),
    ("sbquo", "\u{201a}"),
    ("ldquo", "\u{201c}"),
    ("rdquo", "\u{201d}"),
    ("bdquo", "\u{201e}"),
    ("laquo", "\u{ab}"),
    ("raquo", "\u{bb}"),
    ("lsaquo", "\u{2039}"),
    ("rsaquo", "\u{203a}"),
    ("hellip", "\u{2026}"),
    ("bull", "\u{2022}"),
    ("middot", "\u{b7}"),
    ("dagger", "\u{2020}"),
    ("Dagger", "\u{2021}"),
    ("permil", "\u{2030}"),
    ("prime", "\u{2032}"),
    ("sect", "\u{a7}"),
    ("para", "\u{b6}"),
    ("copy", "\u{a9}"),
    ("reg", "\u{ae}"),
    ("trade", "\u{2122}"),
    ("deg", "\u{b0}"),
    ("plusmn", "\u{b1}"),
    ("times", "\u{d7}"),
    ("divide", "\u{f7}"),
    ("frac14", "\u{bc}"),
    ("frac12", "\u{bd}"),
    ("frac34", "\u{be}"),
    ("micro", "\u{b5}"),
    ("iexcl", "\u{a1}"),
    ("iquest", "\u{bf}"),
    ("larr", "\u{2190}"),
    ("rarr", "\u{2192}"),
    ("uarr", "\u{2191}"),
    ("darr", "\u{2193}"),
    ("euro", "\u{20ac}"),
    ("pound", "\u{a3}"),
    ("yen", "\u{a5}"),
    ("cent", "\u{a2}"),
    ("curren", "\u{a4}"),
    ("aacute", "\u{e1}"),
    ("agrave", "\u{e0}"),
    ("auml", "\u{e4}"),
    ("ccedil", "\u{e7}"),
    ("eacute", "\u{e9}"),
    ("egrave", "\u{e8}"),
    ("ecirc", "\u{ea}"),
    ("iacute", "\u{ed}"),
    ("ntilde", "\u{f1}"),
    ("oacute", "\u{f3}"),
    ("ouml", "\u{f6}"),
    ("uacute", "\u{fa}"),
    ("uuml", "\u{fc}"),
    ("szlig", "\u{df}"),
    ("Eacute", "\u{c9}"),
    ("Auml", "\u{c4}"),
    ("Ouml", "\u{d6}"),
    ("Uuml", "\u{dc}"),
];

/// The longest reference body read between `&` and `;`: `#x10FFFF` and every
/// named reference fit.
const REFERENCE_MAX_CHARS: usize = 32;

/// `text` with its HTML character references decoded: the named ones above,
/// and decimal (`&#39;`) and hexadecimal (`&#x2019;`) numeric ones, a numeric
/// reference to no valid character becoming U+FFFD. An unknown name, or an
/// `&` that does not open a `;`-terminated reference, is left verbatim.
pub fn decode_entities(text: &str) -> String {
    // Read byte by byte: an `&` that opens no reference stays in the run
    // copied whole, so a run dense in them costs no more than other text.
    let mut out = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let (mut copied, mut at) = (0, 0);
    while at < bytes.len() {
        if bytes[at] != b'&' {
            at += 1;
            continue;
        }
        let Some((len, decoded)) = reference_at(text, at) else {
            at += 1;
            continue;
        };
        out.push_str(&text[copied..at]);
        match decoded {
            Reference::Char(c) => out.push(c),
            Reference::Named(text) => out.push_str(text),
        }
        at += len;
        copied = at;
    }
    out.push_str(&text[copied..]);
    out
}

/// The reference opened by the `&` at `at` in `text`: its length through
/// the `;`, and what it stands for; `None` when it opens none this decoder
/// knows.
fn reference_at(text: &str, at: usize) -> Option<(usize, Reference)> {
    let bytes = text.as_bytes();
    let body = at + 1;
    // A body is `#`, letters and digits, so the scan stops at the first other
    // character; a name's key is read in the same scan.
    let mut end = body;
    let mut key = 0u64;
    while let Some(&b) = bytes.get(end)
        && (b.is_ascii_alphanumeric() || b == b'#')
    {
        if end - body == REFERENCE_MAX_CHARS {
            return None;
        }
        key = key << 8 | u64::from(b);
        end += 1;
    }
    if bytes.get(end) != Some(&b';') {
        return None;
    }
    let decoded = match bytes[body] {
        b'#' => numeric(&text[body + 1..end])?,
        _ if end - body > KEY_MAX => return None,
        _ => Reference::Named(NAMED_TABLE.get(key)?),
    };
    Some((end - at + 1, decoded))
}

/// What a reference stands for.
enum Reference {
    /// A numeric reference's character.
    Char(char),
    /// A named reference's text.
    Named(&'static str),
}

/// The character a numeric reference's body after `#` stands for: decimal,
/// or hexadecimal after `x`; U+FFFD for a code point that is no character.
/// `None` when the digits are not digits of the radix.
fn numeric(number: &str) -> Option<Reference> {
    let code = match number.strip_prefix(['x', 'X']) {
        Some(hex) => parse_digits(hex, 16)?,
        None => parse_digits(number, 10)?,
    };
    let decoded = Some(code)
        .filter(|code| *code != 0)
        .and_then(char::from_u32)
        .unwrap_or(char::REPLACEMENT_CHARACTER);
    Some(Reference::Char(decoded))
}

/// The most bytes of a name a [`name_key`] holds.
const KEY_MAX: usize = 8;

/// A name of at most [`KEY_MAX`] bytes as one integer, its bytes in order,
/// ASCII-lowercased when `fold`, so a [`KeyTable`] is searched by comparing
/// integers rather than strings; 0, which no name has, for an empty or a
/// longer name.
const fn name_key(name: &[u8], fold: bool) -> u64 {
    if name.len() > KEY_MAX {
        return 0;
    }
    let mut key = 0;
    let mut at = 0;
    while at < name.len() {
        let b = if fold {
            name[at].to_ascii_lowercase()
        } else {
            name[at]
        };
        key = key << 8 | b as u64;
        at += 1;
    }
    key
}

/// The odd multiplier of a Fibonacci hash: 2^64 divided by the golden ratio.
const FIBONACCI: u64 = 0x9e37_79b9_7f4a_7c15;

/// Names, each with a value, as an open-addressing table of `N` slots built
/// at compile time: each name's [`name_key`] and value in the first free slot
/// from the key's Fibonacci hash, wrapping; a free slot holds key 0. At least
/// half the slots are free, so a search ends within a few.
struct KeyTable<V: Copy, const N: usize>([(u64, V); N]);

impl<V: Copy, const N: usize> KeyTable<V, N> {
    /// `entries` as a table, each name keyed by [`name_key`] with `fold`;
    /// `free` is the value of a free slot, never answered.
    const fn new(entries: &[(&str, V)], fold: bool, free: V) -> Self {
        assert!(N.is_power_of_two() && entries.len() * 2 <= N);
        let mut slots = [(0, free); N];
        let mut at = 0;
        while at < entries.len() {
            let (name, value) = entries[at];
            let key = name_key(name.as_bytes(), fold);
            assert!(key != 0, "a table's name fits a key");
            let mut slot = Self::slot(key);
            while slots[slot].0 != 0 {
                slot = (slot + 1) % N;
            }
            slots[slot] = (key, value);
            at += 1;
        }
        Self(slots)
    }

    /// The slot a search for `key` starts at: the key's Fibonacci hash.
    const fn slot(key: u64) -> usize {
        (key.wrapping_mul(FIBONACCI) >> (64 - N.trailing_zeros())) as usize
    }

    /// The value of the name whose [`name_key`] is `key`; `None` for a name
    /// the table does not hold.
    fn get(&self, key: u64) -> Option<V> {
        let mut slot = Self::slot(key);
        loop {
            match self.0[slot] {
                (0, _) => return None,
                (found, value) if found == key => return Some(value),
                _ => slot = (slot + 1) % N,
            }
        }
    }
}

/// [`NAMED`] as a table, by the case-sensitive [`name_key`].
const NAMED_TABLE: KeyTable<&str, 256> = KeyTable::new(NAMED, false, "");

/// `digits` in `radix`, saturating just past the last code point; `None`
/// when empty or holding a non-digit.
fn parse_digits(digits: &str, radix: u32) -> Option<u32> {
    if digits.is_empty() {
        return None;
    }
    digits.bytes().try_fold(0u32, |value, b| {
        let digit = char::from(b).to_digit(radix)?;
        Some((value * radix + digit).min(0x11_0000))
    })
}

/// What a tag does to the text around it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Break {
    /// Removed, its content kept.
    None,
    /// `<br>`, `<li>`: the text after it starts a new line.
    Line,
    /// A block element or its close: the text after it starts a new
    /// paragraph.
    Paragraph,
    /// `<td>`, `<th>`: a tab before the cell's text.
    Tab,
}

/// What an element this renderer knows does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Its open tag starts a line; its close does nothing.
    Line,
    /// Its open tag and its close each start a paragraph.
    Paragraph,
    /// Its open tag puts a tab before the cell; its close does nothing.
    Tab,
    /// It is dropped with everything it holds; a stray close does nothing.
    Dropped,
}

/// The elements this renderer knows whose names fit a [`name_key`]; the rest
/// are paragraphs ([`LONG_PARAGRAPHS`]).
///
/// The paragraph elements are those the HTML Standard's default style sheet
/// (Rendering, "Flow content", "Sections and headings", "Lists", "Tables",
/// "The fieldset and legend elements", "The details and summary elements")
/// displays as a block, a table, a table caption or a table row. `<li>`,
/// `<dt>` and `<dd>` start a line instead, so a list or a term and its
/// definition read one per line. `<script>`, `<style>` and `<title>` are
/// dropped; the head's other elements (`<meta>`, `<link>`, `<base>`) hold
/// nothing, so its content is dropped without treating `<head>` itself as a
/// span.
const TAGS: [(&str, Kind); 47] = {
    use Kind::{Dropped, Line, Paragraph, Tab};
    [
        ("br", Line),
        ("li", Line),
        ("dt", Line),
        ("dd", Line),
        ("td", Tab),
        ("th", Tab),
        ("script", Dropped),
        ("style", Dropped),
        ("title", Dropped),
        ("address", Paragraph),
        ("article", Paragraph),
        ("aside", Paragraph),
        ("caption", Paragraph),
        ("center", Paragraph),
        ("details", Paragraph),
        ("dialog", Paragraph),
        ("dir", Paragraph),
        ("div", Paragraph),
        ("dl", Paragraph),
        ("fieldset", Paragraph),
        ("figure", Paragraph),
        ("footer", Paragraph),
        ("form", Paragraph),
        ("h1", Paragraph),
        ("h2", Paragraph),
        ("h3", Paragraph),
        ("h4", Paragraph),
        ("h5", Paragraph),
        ("h6", Paragraph),
        ("header", Paragraph),
        ("hgroup", Paragraph),
        ("hr", Paragraph),
        ("legend", Paragraph),
        ("listing", Paragraph),
        ("main", Paragraph),
        ("menu", Paragraph),
        ("nav", Paragraph),
        ("ol", Paragraph),
        ("p", Paragraph),
        ("pre", Paragraph),
        ("search", Paragraph),
        ("section", Paragraph),
        ("summary", Paragraph),
        ("table", Paragraph),
        ("tr", Paragraph),
        ("ul", Paragraph),
        ("xmp", Paragraph),
    ]
};

/// The paragraph elements whose names are longer than a [`name_key`] holds.
const LONG_PARAGRAPHS: [&str; 3] = ["blockquote", "figcaption", "plaintext"];

/// [`TAGS`] as a table, by the lowercased [`name_key`].
const TAG_TABLE: KeyTable<Kind, 128> = KeyTable::new(&TAGS, true, Kind::Line);

/// What the element named `name`, in any case, does; `None` for one this
/// renderer does not know. `key` is the name's lowercased [`name_key`].
fn tag_kind(name: &[u8], key: u64) -> Option<Kind> {
    if name.len() > KEY_MAX {
        let long = LONG_PARAGRAPHS
            .iter()
            .any(|long| long.as_bytes().eq_ignore_ascii_case(name));
        return long.then_some(Kind::Paragraph);
    }
    if key == 0 {
        return None;
    }
    TAG_TABLE.get(key)
}

/// The break a tag of an element of `kind` makes; `close` for `</name>`.
fn tag_break(kind: Option<Kind>, close: bool) -> Break {
    match (kind, close) {
        (Some(Kind::Paragraph), _) => Break::Paragraph,
        (Some(Kind::Line), false) => Break::Line,
        (Some(Kind::Tab), false) => Break::Tab,
        _ => Break::None,
    }
}

/// HTML as text, for a model to read: comments, `<script>`, `<style>` and
/// `<title>` dropped with their content, and so the head's content; `<br>`,
/// `<li>`, `<dt>` and `<dd>` start a line; block elements ([`tag_break`])
/// and their closes start a paragraph; `<td>` and `<th>` put a tab before the
/// cell; every other tag is removed and its content kept — a link keeps its
/// text, not its target. Content that styles or attributes hide is kept.
/// Character references are decoded; whitespace runs in the source are one
/// space; each line is trimmed; blank lines between text are at most one.
/// `<pre>` is not special: its text reads the same with its whitespace
/// collapsed.
///
/// A `<` not followed by a letter, `/`, `!` or `?` is text. A quote in a tag
/// opens a quoted value only right after `=`; elsewhere it is an ordinary
/// character, as in `alt=Bob's`. A comment ends as HTML ends one
/// ([`comment_end`]). A tag, comment or dropped element left open runs to
/// the end of the input, as a browser reads it.
pub fn html_to_text(html: &str) -> String {
    // One pass: the source is read up to each byte that can end a word —
    // ASCII whitespace, `&` or `<` — and the text is written as it is read,
    // each line trimmed as it ends.
    let bytes = html.as_bytes();
    let mut out = TextOut::with_capacity(html.len());
    // Where the word being read began in the source.
    let mut word = 0;
    let mut at = 0;
    loop {
        at = crate::bytes::find(bytes, at, words_ending, ends_a_word);
        let Some(&b) = bytes.get(at) else {
            break;
        };
        if b == b'&' {
            // Most `&` in text are followed by no reference body at all.
            let opens = bytes
                .get(at + 1)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'#');
            let Some((len, decoded)) = opens.then(|| reference_at(html, at)).flatten() else {
                at += 1;
                continue;
            };
            out.piece(&html[word..at]);
            match decoded {
                Reference::Char(c) if c.is_ascii_whitespace() => out.space(),
                Reference::Char(c) => out.piece(c.encode_utf8(&mut [0; 4])),
                Reference::Named(text) => out.piece(text),
            }
            at += len;
            word = at;
            continue;
        }
        if b != b'<' {
            out.piece(&html[word..at]);
            out.space();
            at += 1;
            word = at;
            continue;
        }
        let next = bytes.get(at + 1);
        if !next.is_some_and(|b| b.is_ascii_alphabetic() || matches!(b, b'/' | b'!' | b'?')) {
            at += 1;
            continue;
        }
        out.piece(&html[word..at]);
        let end = if next == Some(&b'!') && bytes.get(at + 2..at + 4) == Some(b"--") {
            comment_end(html, at + 4)
        } else {
            let close = next == Some(&b'/');
            let name = at + 1 + usize::from(close);
            let (name_end, key) = tag_name(bytes, name);
            // The name holds no quote and no `=`, so the scan for the tag's
            // end starts after it; most tags end there.
            let end = if bytes.get(name_end) == Some(&b'>') {
                Some(name_end + 1)
            } else {
                tag_end(bytes, name_end)
            };
            end.and_then(|end| {
                let name = &bytes[name..name_end];
                let kind = tag_kind(name, key);
                if !close && kind == Some(Kind::Dropped) {
                    dropped_end(bytes, name, end)
                } else {
                    out.tag(tag_break(kind, close));
                    Some(end)
                }
            })
        };
        let Some(end) = end else {
            word = html.len();
            break;
        };
        at = end;
        word = end;
    }
    out.piece(&html[word..]);
    out.finish()
}

/// The bytes of a word of eight source bytes that [`ends_a_word`]: ASCII
/// whitespace is a space or 0x09–0x0D other than 0x0B. Every such byte is
/// under `=`, so a word with none under it is passed over at once.
fn words_ending(word: u64) -> u64 {
    use crate::bytes::{below, equal};
    if below(word, b'=') == 0 {
        return 0;
    }
    let controls = below(word, 0x0e) & !below(word, 0x09) & !equal(word, 0x0b);
    controls | equal(word, b' ') | equal(word, b'&') | equal(word, b'<')
}

/// Whether [`html_to_text`] stops at source byte `b`: ASCII whitespace, `&`
/// or `<`.
fn ends_a_word(b: u8) -> bool {
    b.is_ascii_whitespace() || b == b'&' || b == b'<'
}

/// The byte after the end of the comment whose `<!--` ends at `body`; `None`
/// when it is never closed. As HTML's tokenizer reads it: `<!-->` and
/// `<!--->` are whole empty comments, and otherwise the first `-->` or
/// `--!>` ends the comment.
fn comment_end(html: &str, body: usize) -> Option<usize> {
    let rest = &html[body..];
    if rest.starts_with('>') {
        return Some(body + 1);
    }
    if rest.starts_with("->") {
        return Some(body + 2);
    }
    // The first `--` followed by `>` or `!>`. A `-->` and a `--!>` cannot
    // overlap, so the one that starts first also ends first.
    let mut from = 0;
    while let Some(at) = rest.as_bytes()[from..].iter().position(|&b| b == b'-') {
        let dash = from + at;
        let after = &rest[dash + 1..];
        if after.starts_with("->") {
            return Some(body + dash + 3);
        }
        if after.starts_with("-!>") {
            return Some(body + dash + 4);
        }
        from = dash + 1;
    }
    None
}

/// The byte after the `>` closing a tag, scanning from `from`, inside the tag
/// and after its name, with quoted attribute values skipped; `None` when the
/// tag is never closed. A quote opens a value only when the last
/// non-whitespace character before it is `=`.
fn tag_end(bytes: &[u8], from: usize) -> Option<usize> {
    let mut quote = None;
    let mut after_equals = false;
    for (i, &b) in bytes[from..].iter().enumerate() {
        match (quote, b) {
            (Some(q), b) if b == q => quote = None,
            (Some(_), _) => {}
            (None, b'"' | b'\'') if after_equals => quote = Some(b),
            (None, b'>') => return Some(from + i + 1),
            (None, _) => {}
        }
        if quote.is_none() && !b.is_ascii_whitespace() {
            after_equals = b == b'=';
        }
    }
    None
}

/// The end of the tag name that starts at `name` — its ASCII letters and
/// digits — and the name's lowercased [`name_key`], read in the same scan. `<!…>`,
/// `<?…>` and `</…` followed by neither have an empty name.
fn tag_name(bytes: &[u8], name: usize) -> (usize, u64) {
    let mut key = 0u64;
    let mut at = name;
    while let Some(&b) = bytes.get(at)
        && b.is_ascii_alphanumeric()
    {
        key = key << 8 | u64::from(b | 0x20);
        at += 1;
    }
    // A key holds `KEY_MAX` bytes; a longer name has none.
    (at, if at - name > KEY_MAX { 0 } else { key })
}

/// The byte after the close of the dropped element `name` whose open tag
/// ends at `from`, the close's name matched in any case; `None` when
/// it is never closed.
fn dropped_end(bytes: &[u8], name: &[u8], from: usize) -> Option<usize> {
    let mut at = from;
    while let Some(found) = bytes[at..].windows(2).position(|pair| pair == b"</") {
        let close = at + found;
        let after = &bytes[close + 2..];
        if after
            .get(..name.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(name))
        {
            return tag_end(bytes, close + 2);
        }
        at = close + 2;
    }
    None
}

/// Whether a piece of text with no ASCII whitespace whose first or last byte
/// is `b` may have something `str::trim` removes at that end: a byte of a
/// longer character, or U+000B, which `str::trim` removes and ASCII
/// whitespace leaves out.
fn trimmed_at(b: u8) -> bool {
    !b.is_ascii() || b == 0x0b
}

/// The text being written by [`html_to_text`], a line at a time: a line's
/// leading whitespace is never written, its trailing whitespace is cut when
/// the line ends, and the newlines before a line are written only once text
/// follows them.
struct TextOut {
    text: String,
    /// Source whitespace was seen since the last character written.
    space: bool,
    /// The current line holds text other than whitespace.
    open: bool,
    /// The end of the current line's last character other than whitespace.
    content_end: usize,
    /// The newlines owed before the next line's text: one, or two when a
    /// blank line comes between.
    newlines: u8,
}

impl TextOut {
    fn with_capacity(capacity: usize) -> TextOut {
        TextOut {
            text: String::with_capacity(capacity),
            space: false,
            open: false,
            content_end: 0,
            newlines: 0,
        }
    }

    /// Source whitespace: one space before the next text on the line.
    fn space(&mut self) {
        self.space = true;
    }

    /// Text holding no ASCII whitespace, written after what came before it
    /// with no space between unless [`TextOut::space`] came between.
    fn piece(&mut self, piece: &str) {
        if piece.is_empty() {
            return;
        }
        let piece = if self.open {
            if self.space && self.text.as_bytes().last() != Some(&b'\t') {
                self.text.push(' ');
            }
            piece
        } else {
            // What `str::trim` removes at a line's start is never written.
            let piece = if trimmed_at(piece.as_bytes()[0]) {
                piece.trim_start()
            } else {
                piece
            };
            if piece.is_empty() {
                self.space = false;
                return;
            }
            if !self.text.is_empty() {
                self.text
                    .push_str(if self.newlines > 1 { "\n\n" } else { "\n" });
            }
            self.open = true;
            piece
        };
        self.space = false;
        self.text.push_str(piece);
        let last = piece.as_bytes()[piece.len() - 1];
        let content = if trimmed_at(last) {
            piece.trim_end().len()
        } else {
            piece.len()
        };
        if content > 0 {
            self.content_end = self.text.len() - (piece.len() - content);
        }
    }

    fn tag(&mut self, kind: Break) {
        let newlines = match kind {
            Break::None => return,
            Break::Line => 1,
            Break::Paragraph => 2,
            Break::Tab => {
                if self.open {
                    self.text.push('\t');
                }
                self.space = false;
                return;
            }
        };
        self.space = false;
        if self.open {
            self.text.truncate(self.content_end);
            self.open = false;
            self.newlines = newlines;
        } else {
            self.newlines = (self.newlines + newlines).min(2);
        }
    }

    /// The text, its last line trimmed.
    fn finish(mut self) -> String {
        if self.open {
            self.text.truncate(self.content_end);
        }
        self.text.shrink_to_fit();
        self.text
    }
}

/// Gmail's `internalDate` — milliseconds since the Unix epoch, as a decimal
/// string — as an RFC 3339 UTC instant to the second
/// (`2026-10-31T14:05:09Z`). `None` when the string is not a non-negative
/// decimal integer that fits `u64`.
pub fn internal_date(epoch_ms: &str) -> Option<String> {
    if epoch_ms.is_empty() || !epoch_ms.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let ms: u64 = epoch_ms.parse().ok()?;
    Some(brenn_cal::ms_to_rfc3339_seconds(ms))
}

/// The bytes `data` encodes in base64url — `+` and `/` accepted for `-` and
/// `_`, `=` padding and whitespace ignored. `None` for any other character,
/// or a final group of one character, which encodes no whole byte.
pub fn base64url(data: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() / 4 * 3 + 2);
    let mut decoder = Base64Url::default();
    let read = decoder.push(data.as_bytes(), &mut out);
    (read == data.len() && decoder.whole()).then_some(out)
}

/// One input byte's class in base64url: its six bits, [`B64_SKIP`], or
/// [`B64_STOP`].
const B64_SKIP: u8 = 64;
const B64_STOP: u8 = 255;

/// Each byte's class: either alphabet, `=` padding and the four whitespace
/// characters skipped, everything else a stop.
const B64_CLASS: [u8; 256] = {
    let mut table = [B64_STOP; 256];
    let mut i = 0;
    while i < 26 {
        table[b'A' as usize + i] = i as u8;
        table[b'a' as usize + i] = 26 + i as u8;
        i += 1;
    }
    let mut d = 0;
    while d < 10 {
        table[b'0' as usize + d] = 52 + d as u8;
        d += 1;
    }
    table[b'-' as usize] = 62;
    table[b'+' as usize] = 62;
    table[b'_' as usize] = 63;
    table[b'/' as usize] = 63;
    table[b'=' as usize] = B64_SKIP;
    table[b' ' as usize] = B64_SKIP;
    table[b'\t' as usize] = B64_SKIP;
    table[b'\r' as usize] = B64_SKIP;
    table[b'\n' as usize] = B64_SKIP;
    table
};

/// A push decoder of base64url, by [`base64url`]'s rules, fed in pieces split
/// anywhere; it holds at most one partial group's bits between pieces.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Base64Url {
    bits: u32,
    held: u32,
    /// The position in the current group of four characters.
    group: u8,
}

impl Base64Url {
    /// Decode `input` up to its first byte that is neither in either alphabet
    /// nor skipped, appending the bytes to `out`: the index of that byte, or
    /// `input.len()` when there is none.
    pub fn push(&mut self, input: &[u8], out: &mut Vec<u8>) -> usize {
        let mut i = 0;
        while i < input.len() {
            if self.group == 0 {
                i += whole_groups(&input[i..], out);
                let Some(&b) = input.get(i) else {
                    break;
                };
                if !self.one(b, out) {
                    return i;
                }
            } else if !self.one(input[i], out) {
                return i;
            }
            i += 1;
        }
        input.len()
    }

    /// Decode one input byte; false when it is a stop.
    fn one(&mut self, b: u8, out: &mut Vec<u8>) -> bool {
        match B64_CLASS[usize::from(b)] {
            B64_SKIP => {}
            B64_STOP => return false,
            value => {
                self.bits = (self.bits << 6) | u32::from(value);
                self.held += 6;
                self.group = (self.group + 1) % 4;
                if self.held >= 8 {
                    self.held -= 8;
                    out.push((self.bits >> self.held) as u8);
                    self.bits &= (1 << self.held) - 1;
                }
            }
        }
        true
    }

    /// Whether the input may end here: not after a final group of one
    /// character, which encodes no whole byte.
    pub fn whole(&self) -> bool {
        self.group != 1
    }
}

/// Decode the whole groups of four alphabet characters `input` starts
/// with, three bytes each, appending them to `out`; the bytes read. Read at a
/// group's start, where the decoder holds no bits, they decode as one
/// character at a time would.
fn whole_groups(input: &[u8], out: &mut Vec<u8>) -> usize {
    // Each group's three bytes are written as one four-byte word into room
    // made for every group and one byte more; the fourth byte is written
    // over by the next group, and what no group filled is cut off after.
    let start = out.len();
    out.resize(start + input.len() / 4 * 3 + 1, 0);
    let (mut read, mut written) = (0, start);
    for group in input.chunks_exact(4) {
        let word = B64_GROUP[0][usize::from(group[0])]
            | B64_GROUP[1][usize::from(group[1])]
            | B64_GROUP[2][usize::from(group[2])]
            | B64_GROUP[3][usize::from(group[3])];
        if word & B64_GROUP_STOP != 0 {
            break;
        }
        out[written..written + 4].copy_from_slice(&word.to_le_bytes());
        read += 4;
        written += 3;
    }
    out.truncate(written);
    read
}

/// In [`B64_GROUP`], a byte that is no sextet: a skip or a stop.
const B64_GROUP_STOP: u32 = 1 << 31;

/// For each place in a group of four characters, each byte's sextet where
/// its bits fall in the group's three bytes, the first byte lowest, so a
/// group's bytes are its four entries ORed; [`B64_GROUP_STOP`] for a byte
/// that is no sextet.
const B64_GROUP: [[u32; 256]; 4] = {
    let mut tables = [[B64_GROUP_STOP; 256]; 4];
    let mut b = 0;
    while b < 256 {
        if B64_CLASS[b] < B64_SKIP {
            let mut place = 0;
            while place < 4 {
                let bits = (B64_CLASS[b] as u32) << (18 - 6 * place);
                tables[place][b] = bits >> 16 | (bits >> 8 & 0xff) << 8 | (bits & 0xff) << 16;
                place += 1;
            }
        }
        b += 1;
    }
    tables
};

/// Windows-1252's 0x80–0x9F, where it differs from ISO-8859-1; U+FFFD for
/// the five bytes it leaves undefined.
const CP1252_HIGH: [&str; 32] = [
    "\u{20ac}", "\u{fffd}", "\u{201a}", "\u{0192}", "\u{201e}", "\u{2026}", "\u{2020}", "\u{2021}",
    "\u{02c6}", "\u{2030}", "\u{0160}", "\u{2039}", "\u{0152}", "\u{fffd}", "\u{017d}", "\u{fffd}",
    "\u{fffd}", "\u{2018}", "\u{2019}", "\u{201c}", "\u{201d}", "\u{2022}", "\u{2013}", "\u{2014}",
    "\u{02dc}", "\u{2122}", "\u{0161}", "\u{203a}", "\u{0153}", "\u{fffd}", "\u{017e}", "\u{0178}",
];

/// 0xA0–0xFF in ISO-8859-1 and Windows-1252 alike: U+00A0–U+00FF, two bytes
/// each.
const LATIN1_HIGH: &str = concat!(
    "\u{a0}\u{a1}\u{a2}\u{a3}\u{a4}\u{a5}\u{a6}\u{a7}\u{a8}\u{a9}\u{aa}\u{ab}\u{ac}\u{ad}",
    "\u{ae}\u{af}\u{b0}\u{b1}\u{b2}\u{b3}\u{b4}\u{b5}\u{b6}\u{b7}\u{b8}\u{b9}\u{ba}\u{bb}",
    "\u{bc}\u{bd}\u{be}\u{bf}\u{c0}\u{c1}\u{c2}\u{c3}\u{c4}\u{c5}\u{c6}\u{c7}\u{c8}\u{c9}",
    "\u{ca}\u{cb}\u{cc}\u{cd}\u{ce}\u{cf}\u{d0}\u{d1}\u{d2}\u{d3}\u{d4}\u{d5}\u{d6}\u{d7}",
    "\u{d8}\u{d9}\u{da}\u{db}\u{dc}\u{dd}\u{de}\u{df}\u{e0}\u{e1}\u{e2}\u{e3}\u{e4}\u{e5}",
    "\u{e6}\u{e7}\u{e8}\u{e9}\u{ea}\u{eb}\u{ec}\u{ed}\u{ee}\u{ef}\u{f0}\u{f1}\u{f2}\u{f3}",
    "\u{f4}\u{f5}\u{f6}\u{f7}\u{f8}\u{f9}\u{fa}\u{fb}\u{fc}\u{fd}\u{fe}\u{ff}",
);

/// Something about a message's decoding the reader is told, rather than shown
/// wrong text silently. Strings quoted are the sender's, verbatim, borrowed
/// from the message's tree where it holds them unescaped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Note<'a> {
    /// A text part's declared charset is not one decoded here; it was read as
    /// UTF-8.
    UnknownCharset(Cow<'a, str>),
    /// A text part declared as UTF-8 or ASCII held invalid bytes.
    InvalidUtf8,
    /// A `multipart/*` or `message/rfc822` part at `depth` (the payload is 0)
    /// came without its children.
    UnlistedChildren {
        mime_type: Cow<'a, str>,
        depth: usize,
    },
}

/// Which kind of part a text came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Plain,
    Html,
}

/// A part listed rather than read: what the sender attached, or a non-text
/// part of the body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attachment {
    /// The part's `filename`, `""` when Google gives none.
    pub filename: String,
    pub mime_type: String,
    /// `body.size`, when Google gives it.
    pub size: Option<u64>,
    /// Shown in the body (`Content-Disposition: inline` or a `Content-ID`),
    /// not attached beside it.
    pub inline: bool,
    /// The part's `partId`, when the part has a `body.attachmentId`, the id
    /// is a part id ([`super::is_part_id`]) and the part is at most
    /// [`super::ATTACHMENT_PART_DEPTH`] levels below the payload: the name
    /// the part is fetched by. `None` for a part carried inside the message,
    /// nested deeper, or without such an id.
    pub part_id: Option<String>,
}

/// What a MIME tree yields: its text parts in reading order, every part
/// listed as an attachment, and the decoding notes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Extracted<'a> {
    pub text_parts: Vec<(String, Source)>,
    pub attachments: Vec<Attachment>,
    pub notes: Vec<Note<'a>>,
    /// A `text/plain` or `text/html` part came as an attachment reference
    /// (`body.attachmentId`, no `data`) and was listed, not read.
    pub text_by_reference: bool,
}

/// Why a MIME tree could not be read.
#[derive(Debug, PartialEq, Eq)]
pub enum WalkError {
    /// A part is not of Gmail's shape.
    Shape,
    /// A part's `body.data` is not base64url; the part's `partId`, or its
    /// MIME type when it has none.
    Base64(String),
}

/// One part of a `format=full` message's MIME tree, as Gmail gives it in
/// `payload` and `parts`, holding only what the walk reads. Every other key
/// and every other header is skipped without being kept. A field of the
/// wrong type, a part, `body` or header entry that is not an object, a
/// header entry without a string `name` and `value`, or a key given twice
/// is refused.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Part<'a> {
    /// `mimeType`, as declared.
    mime_type: Cow<'a, str>,
    /// `body.data`, base64url.
    data: Option<Cow<'a, str>>,
    /// `parts`, `None` when Google gives none.
    parts: Option<Box<[Part<'a>]>>,
    /// `body.size`.
    size: Option<u64>,
    /// What not every part carries, apart, so that a part without it holds
    /// no room for it; `None` when the part gives none of its keys.
    meta: Option<Box<Meta<'a>>>,
    /// `body.attachmentId` is present.
    attachment_id: bool,
}

/// A part's `filename`, `partId` and the headers the walk reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Meta<'a> {
    /// `filename`, `""` when Google gives none.
    filename: Cow<'a, str>,
    part_id: Option<Cow<'a, str>>,
    headers: Headers<'a>,
}

/// The values of the headers the walk reads, each the first entry of the
/// part's `headers` whose name matches case-insensitively.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Headers<'a> {
    content_type: Option<Cow<'a, str>>,
    content_disposition: Option<Cow<'a, str>>,
    content_id: Option<Cow<'a, str>>,
}

/// The headers of a part that has none the walk reads.
const NO_HEADERS: Headers<'static> = Headers {
    content_type: None,
    content_disposition: None,
    content_id: None,
};

impl<'a> Part<'a> {
    /// `payload`, Gmail's part tree as a JSON value, its strings borrowed
    /// from it.
    pub fn from_value(payload: &'a Value) -> Result<Part<'a>, WalkError> {
        Part::deserialize(payload).map_err(|_| WalkError::Shape)
    }

    /// `filename`, `""` when Google gives none.
    fn filename(&self) -> &str {
        self.meta.as_ref().map_or("", |meta| &meta.filename)
    }

    fn part_id(&self) -> Option<&str> {
        self.meta.as_ref().and_then(|meta| meta.part_id.as_deref())
    }

    fn headers(&self) -> &Headers<'a> {
        self.meta.as_ref().map_or(&NO_HEADERS, |meta| &meta.headers)
    }
}

/// A `format=full` message as Gmail answers it, holding only its `id` and its
/// part tree, whose strings are borrowed from the answer. Every other key is
/// skipped without being kept; a missing or mistyped `id` or `payload`, or a
/// key given twice, is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message<'a> {
    pub id: String,
    pub payload: Part<'a>,
}

impl<'a> Message<'a> {
    /// `bytes`, Gmail's answer as JSON text. Anything else, and an answer not
    /// of Gmail's shape, is [`WalkError::Shape`].
    pub fn from_slice(bytes: &'a [u8]) -> Result<Message<'a>, WalkError> {
        // JSON is UTF-8. Checked once here, the parse reads each string
        // without checking it again.
        let text = std::str::from_utf8(bytes).map_err(|_| WalkError::Shape)?;
        serde_json::from_str(text).map_err(|_| WalkError::Shape)
    }
}

impl<'de> Deserialize<'de> for Message<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MessageVisitor;
        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = Message<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a Gmail message")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Message<'de>, A::Error> {
                let (mut id, mut payload) = (None, None);
                while let Some(key) = map.next_key::<Text>()? {
                    match &*key.0 {
                        "id" => once(&mut id, map.next_value::<String>()?, "id")?,
                        "payload" => once(&mut payload, map.next_value::<Part>()?, "payload")?,
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(Message {
                    id: id.ok_or_else(|| de::Error::missing_field("id"))?,
                    payload: payload.ok_or_else(|| de::Error::missing_field("payload"))?,
                })
            }
        }
        deserializer.deserialize_map(MessageVisitor)
    }
}

/// A JSON string, borrowed from the input where it holds no escape.
struct Text<'de>(Cow<'de, str>);

impl<'de> Deserialize<'de> for Text<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor;
        impl<'de> Visitor<'de> for TextVisitor {
            type Value = Text<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string")
            }
            fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Text<'de>, E> {
                Ok(Text(Cow::Borrowed(v)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Text<'de>, E> {
                Ok(Text(Cow::Owned(v.to_owned())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Text<'de>, E> {
                Ok(Text(Cow::Owned(v)))
            }
        }
        deserializer.deserialize_str(TextVisitor)
    }
}

/// `value` into `slot`, refusing a key seen before.
fn once<T, E: de::Error>(slot: &mut Option<T>, value: T, key: &'static str) -> Result<(), E> {
    if slot.replace(value).is_some() {
        return Err(E::duplicate_field(key));
    }
    Ok(())
}

impl<'de> Deserialize<'de> for Part<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PartVisitor;
        impl<'de> Visitor<'de> for PartVisitor {
            type Value = Part<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a Gmail message part")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Part<'de>, A::Error> {
                let (mut mime_type, mut filename, mut part_id) = (None, None, None);
                let (mut headers, mut body, mut parts) = (None, None, None);
                while let Some(key) = map.next_key::<Text>()? {
                    match &*key.0 {
                        "mimeType" => {
                            once(&mut mime_type, map.next_value::<Text>()?.0, "mimeType")?;
                        }
                        "filename" => {
                            once(&mut filename, map.next_value::<Text>()?.0, "filename")?;
                        }
                        "partId" => once(&mut part_id, map.next_value::<Text>()?.0, "partId")?,
                        "headers" => once(&mut headers, map.next_value()?, "headers")?,
                        "body" => once(&mut body, map.next_value::<Body>()?, "body")?,
                        "parts" => once(&mut parts, map.next_value()?, "parts")?,
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                let body = body.unwrap_or_default();
                let meta =
                    (filename.is_some() || part_id.is_some() || headers.is_some()).then(|| {
                        Box::new(Meta {
                            filename: filename.unwrap_or_default(),
                            part_id,
                            headers: headers.unwrap_or_default(),
                        })
                    });
                Ok(Part {
                    mime_type: mime_type.ok_or_else(|| de::Error::missing_field("mimeType"))?,
                    data: body.data,
                    parts: parts.map(Vec::<Part>::into_boxed_slice),
                    size: body.size,
                    meta,
                    attachment_id: body.attachment_id,
                })
            }
        }
        deserializer.deserialize_map(PartVisitor)
    }
}

/// A part's `body`.
#[derive(Default)]
struct Body<'a> {
    attachment_id: bool,
    size: Option<u64>,
    data: Option<Cow<'a, str>>,
}

impl<'de> Deserialize<'de> for Body<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BodyVisitor;
        impl<'de> Visitor<'de> for BodyVisitor {
            type Value = Body<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a Gmail part body")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Body<'de>, A::Error> {
                let (mut attachment_id, mut size, mut data) = (None, None, None);
                while let Some(key) = map.next_key::<Text>()? {
                    match &*key.0 {
                        "attachmentId" => {
                            let Text(_) = map.next_value()?;
                            once(&mut attachment_id, (), "attachmentId")?;
                        }
                        "size" => once(&mut size, map.next_value()?, "size")?,
                        "data" => once(&mut data, map.next_value::<Text>()?.0, "data")?,
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(Body {
                    attachment_id: attachment_id.is_some(),
                    size,
                    data,
                })
            }
        }
        deserializer.deserialize_map(BodyVisitor)
    }
}

impl<'de> Deserialize<'de> for Headers<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct HeadersVisitor;
        impl<'de> Visitor<'de> for HeadersVisitor {
            type Value = Headers<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("Gmail's header list")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Headers<'de>, A::Error> {
                let mut headers = Headers::default();
                while let Some(HeaderEntry { name, value }) = seq.next_element()? {
                    let slot = if name.eq_ignore_ascii_case("Content-Type") {
                        &mut headers.content_type
                    } else if name.eq_ignore_ascii_case("Content-Disposition") {
                        &mut headers.content_disposition
                    } else if name.eq_ignore_ascii_case("Content-ID") {
                        &mut headers.content_id
                    } else {
                        continue;
                    };
                    if slot.is_none() {
                        *slot = Some(value);
                    }
                }
                Ok(headers)
            }
        }
        deserializer.deserialize_seq(HeadersVisitor)
    }
}

/// One `{ "name", "value" }` entry of a header list.
struct HeaderEntry<'de> {
    name: Cow<'de, str>,
    value: Cow<'de, str>,
}

impl<'de> Deserialize<'de> for HeaderEntry<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EntryVisitor;
        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = HeaderEntry<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a header entry")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<HeaderEntry<'de>, A::Error> {
                let (mut name, mut value) = (None, None);
                while let Some(key) = map.next_key::<Text>()? {
                    match &*key.0 {
                        "name" => once(&mut name, map.next_value::<Text>()?.0, "name")?,
                        "value" => once(&mut value, map.next_value::<Text>()?.0, "value")?,
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(HeaderEntry {
                    name: name.ok_or_else(|| de::Error::missing_field("name"))?,
                    value: value.ok_or_else(|| de::Error::missing_field("value"))?,
                })
            }
        }
        deserializer.deserialize_map(EntryVisitor)
    }
}

/// The forms a caller asked a message's body for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Wanted {
    /// The text view's parts, as [`extract`] yields them.
    pub text: bool,
    /// The HTML view's leaves, decoded.
    pub html: bool,
    /// The HTML view's leaves read by [`html_to_text`].
    pub rendered: bool,
}

/// A text leaf of the tree, decoded once and shared by the views that hold
/// it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leaf {
    pub source: Source,
    /// A `text/plain` leaf's text or a `text/html` leaf's markup, decoded in
    /// its charset; `None` when no form asked for reads it.
    pub decoded: Option<String>,
    /// A `text/html` leaf read by [`html_to_text`]; `None` for a
    /// `text/plain` leaf, or when no form asked for reads it.
    pub rendering: Option<String>,
}

impl Leaf {
    /// The leaf as the text view reads it: a plain leaf's text, an HTML
    /// leaf's rendering.
    pub fn text(&self) -> Option<&str> {
        match self.source {
            Source::Plain => self.decoded.as_deref(),
            Source::Html => self.rendering.as_deref(),
        }
    }
}

/// What a MIME tree yields for the forms asked: its text leaves, and the
/// two views over them as indices into `leaves`, in reading order. Its
/// decoding notes are answered beside it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bodies {
    pub leaves: Vec<Leaf>,
    /// The text view, as [`extract`] chooses it; empty unless `text` was
    /// asked for.
    pub text: Vec<usize>,
    /// The HTML view: the `text/html` leaves of the tree walked as the text
    /// view is, except that a `multipart/alternative` takes its first child
    /// holding a `text/html` leaf whose markup is not all whitespace, and
    /// nothing when none does. Empty unless `html` or `rendered` was asked
    /// for.
    pub html: Vec<usize>,
    /// How many `text/plain` or `text/html` parts came as an attachment
    /// reference (`body.attachmentId`, no `filename`), in no view.
    pub by_reference: usize,
}

/// A message's MIME tree, `payload` and its `parts` as Gmail gives them,
/// read into text, attachments and notes.
///
/// A part with a non-empty `filename` or a `body.attachmentId` is an
/// attachment. `multipart/alternative` takes the first child yielding
/// `text/plain` text, else the first yielding `text/html`, else the first
/// yielding any text, and lists every child's attachments. Every other
/// `multipart/*` and `message/rfc822` yields its children's in order; one
/// with no `parts` yields nothing and a note. A `text/plain` leaf is decoded
/// in its charset; a `text/html` leaf is decoded in its charset and read by
/// [`html_to_text`]. A text leaf with no `data` is empty and yields nothing;
/// any other leaf is an attachment.
pub fn extract<'a>(payload: &Part<'a>) -> Result<Extracted<'a>, WalkError> {
    let wanted = Wanted {
        text: true,
        ..Wanted::default()
    };
    let mut attachments = Vec::new();
    let (mut walked, notes) = read(payload, wanted, Some(&mut attachments))?;
    let text_parts = walked
        .text
        .iter()
        .map(|&leaf| {
            let leaf = &mut walked.leaves[leaf];
            let text = match leaf.source {
                Source::Plain => leaf.decoded.take(),
                Source::Html => leaf.rendering.take(),
            };
            (text.unwrap_or_default(), leaf.source)
        })
        .collect();
    Ok(Extracted {
        text_parts,
        attachments,
        notes,
        text_by_reference: walked.by_reference > 0,
    })
}

/// A message's MIME tree read into the leaves and views the forms in
/// `wanted` need, by [`extract`]'s rules for the text view and
/// [`Bodies::html`]'s for the HTML view, in one walk. An HTML leaf is read by
/// [`html_to_text`] at most once, and only when a form asked for needs it:
/// for `text` every HTML leaf the walk meets, since the text view's choice
/// of alternative depends on it; for `rendered` alone, those of the HTML
/// view. What no form asked for reads is dropped before this returns.
///
/// Beside it, the decoding notes of the leaves the views asked for hold,
/// and every unlisted-children note, in the order of the tree.
pub fn bodies<'a>(
    payload: &Part<'a>,
    wanted: Wanted,
) -> Result<(Bodies, Vec<Note<'a>>), WalkError> {
    read(payload, wanted, None)
}

/// A leaf as the walk holds it.
struct Held {
    leaf: Leaf,
    /// The markup of an HTML leaf is all whitespace.
    blank: bool,
}

/// The walk's state across a tree whose strings live for `'t`.
struct Walk<'a, 't> {
    wanted: Wanted,
    held: Vec<Held>,
    /// Each note, with the leaf it is about; an unlisted-children note is
    /// about none.
    notes: Vec<(Note<'t>, Option<usize>)>,
    /// The notes of the leaf being decoded, kept empty between leaves.
    leaf_notes: Vec<Note<'t>>,
    attachments: Option<&'a mut Vec<Attachment>>,
    by_reference: usize,
    /// The text view of what has been walked, in reading order. A subtree
    /// appends its view, so each subtree's view is one run of it.
    text: Vec<usize>,
    /// The HTML view, as `text` is kept.
    html: Vec<usize>,
    /// The runs of `text` and `html` that the children of the alternatives
    /// being walked yield, the innermost alternative's last.
    children: Vec<(Range<usize>, Range<usize>)>,
}

fn read<'t>(
    payload: &Part<'t>,
    wanted: Wanted,
    attachments: Option<&mut Vec<Attachment>>,
) -> Result<(Bodies, Vec<Note<'t>>), WalkError> {
    let mut walk = Walk {
        wanted,
        held: Vec::new(),
        notes: Vec::new(),
        leaf_notes: Vec::new(),
        attachments,
        by_reference: 0,
        text: Vec::new(),
        html: Vec::new(),
        children: Vec::new(),
    };
    walk.part(payload, 0)?;
    let mut in_text = vec![false; walk.held.len()];
    let mut in_html = vec![false; walk.held.len()];
    for &leaf in &walk.text {
        in_text[leaf] = true;
    }
    for &leaf in &walk.html {
        in_html[leaf] = true;
    }
    let mut leaves = Vec::with_capacity(walk.held.len());
    for (index, Held { mut leaf, .. }) in walk.held.into_iter().enumerate() {
        let (text, html) = (in_text[index], in_html[index]);
        let keep_decoded = match leaf.source {
            Source::Plain => text,
            Source::Html => html && wanted.html,
        };
        let keep_rendering = match leaf.source {
            Source::Plain => false,
            Source::Html => text || (html && wanted.rendered),
        };
        if keep_rendering && leaf.rendering.is_none() {
            leaf.rendering = leaf.decoded.as_deref().map(html_to_text);
        }
        if !keep_decoded {
            leaf.decoded = None;
        }
        if !keep_rendering {
            leaf.rendering = None;
        }
        leaves.push(leaf);
    }
    let notes = walk
        .notes
        .into_iter()
        .filter(|(_, leaf)| leaf.is_none_or(|leaf| in_text[leaf] || in_html[leaf]))
        .map(|(note, _)| note)
        .collect();
    Ok((
        Bodies {
            leaves,
            text: walk.text,
            html: walk.html,
            by_reference: walk.by_reference,
        },
        notes,
    ))
}

/// Whether `text` starts with `prefix`, which is lowercase, in any case.
fn starts_with_ignoring_case(text: &str, prefix: &str) -> bool {
    text.as_bytes()
        .get(..prefix.len())
        .is_some_and(|start| start.eq_ignore_ascii_case(prefix.as_bytes()))
}

/// `list` from `start` on cut to the run `kept` of it, or to nothing.
fn keep_run(list: &mut Vec<usize>, start: usize, kept: Option<Range<usize>>) {
    match kept {
        Some(run) => {
            let len = run.len();
            list.copy_within(run, start);
            list.truncate(start + len);
        }
        None => list.truncate(start),
    }
}

impl<'t> Walk<'_, 't> {
    /// Walk `part`, appending its views to `text` and `html`.
    fn part(&mut self, part: &Part<'t>, depth: usize) -> Result<(), WalkError> {
        let mime_type = &*part.mime_type;
        let is_html = mime_type.eq_ignore_ascii_case("text/html");
        let is_text = is_html || mime_type.eq_ignore_ascii_case("text/plain");
        let is_alternative = mime_type.eq_ignore_ascii_case("multipart/alternative");
        let is_container = is_alternative
            || starts_with_ignoring_case(mime_type, "multipart/")
            || mime_type.eq_ignore_ascii_case("message/rfc822");

        if !part.filename().is_empty() || part.attachment_id || (!is_text && !is_container) {
            if is_text && part.filename().is_empty() {
                self.by_reference += 1;
            }
            if let Some(attachments) = self.attachments.as_deref_mut() {
                attachments.push(attachment(part, depth));
            }
            return Ok(());
        }

        if is_container {
            let Some(children) = &part.parts else {
                self.notes.push((
                    Note::UnlistedChildren {
                        mime_type: part.mime_type.clone(),
                        depth,
                    },
                    None,
                ));
                return Ok(());
            };
            if !is_alternative {
                for child in children {
                    self.part(child, depth + 1)?;
                }
                return Ok(());
            }
            let (text, html) = (self.text.len(), self.html.len());
            let mark = self.children.len();
            for child in children {
                let (child_text, child_html) = (self.text.len(), self.html.len());
                self.part(child, depth + 1)?;
                self.children
                    .push((child_text..self.text.len(), child_html..self.html.len()));
            }
            let chosen_text = self.choose_text(&self.children[mark..]);
            let chosen_html = self.children[mark..]
                .iter()
                .find(|(_, run)| {
                    self.html[run.clone()]
                        .iter()
                        .any(|&leaf| !self.held[leaf].blank)
                })
                .map(|(_, run)| run.clone());
            self.children.truncate(mark);
            keep_run(&mut self.text, text, chosen_text);
            keep_run(&mut self.html, html, chosen_html);
            return Ok(());
        }

        let Some(data) = &part.data else {
            return Ok(());
        };
        let bytes = base64url(data).ok_or_else(|| {
            WalkError::Base64(match part.part_id() {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => mime_type.to_ascii_lowercase(),
            })
        })?;
        let index = self.held.len();
        let declared = match &part.headers().content_type {
            Some(Cow::Borrowed(value)) => charset(value).map(Cow::Borrowed),
            Some(Cow::Owned(value)) => charset(value).map(|name| Cow::Owned(name.to_string())),
            None => None,
        };
        let decoded = decode_charset(bytes, declared, &mut self.leaf_notes);
        self.notes
            .extend(self.leaf_notes.drain(..).map(|note| (note, Some(index))));
        let held = if is_html {
            let blank = decoded.trim().is_empty();
            let rendering = self.wanted.text.then(|| html_to_text(&decoded));
            let keep = self.wanted.html || (self.wanted.rendered && rendering.is_none());
            if self.wanted.text {
                self.text.push(index);
            }
            if self.wanted.html || self.wanted.rendered {
                self.html.push(index);
            }
            Held {
                leaf: Leaf {
                    source: Source::Html,
                    decoded: keep.then_some(decoded),
                    rendering,
                },
                blank,
            }
        } else {
            if self.wanted.text {
                self.text.push(index);
            }
            Held {
                leaf: Leaf {
                    source: Source::Plain,
                    decoded: self.wanted.text.then_some(decoded),
                    rendering: None,
                },
                blank: false,
            }
        };
        self.held.push(held);
        Ok(())
    }

    /// The text view of the alternative child the text view reads, from the
    /// runs of `children`: the first whose text view holds a `text/plain`
    /// leaf of more than whitespace, else the first holding such a
    /// `text/html` rendering, else the first holding any leaf. A blank plain
    /// part does not hide its HTML sibling.
    fn choose_text(&self, children: &[(Range<usize>, Range<usize>)]) -> Option<Range<usize>> {
        let yields = |source: Option<Source>| {
            children
                .iter()
                .find(|(run, _)| {
                    self.text[run.clone()].iter().any(|&leaf| {
                        let leaf = &self.held[leaf].leaf;
                        match source {
                            Some(source) => {
                                leaf.source == source
                                    && leaf.text().is_some_and(|text| !text.trim().is_empty())
                            }
                            None => true,
                        }
                    })
                })
                .map(|(run, _)| run.clone())
        };
        yields(Some(Source::Plain))
            .or_else(|| yields(Some(Source::Html)))
            .or_else(|| yields(None))
    }
}

/// An attachment part as listed.
fn attachment(part: &Part<'_>, depth: usize) -> Attachment {
    let headers = part.headers();
    let inline = headers
        .content_disposition
        .as_deref()
        .is_some_and(|d| d.trim_start().to_ascii_lowercase().starts_with("inline"))
        || headers.content_id.is_some();
    let part_id = part
        .part_id()
        .filter(|id| {
            part.attachment_id && depth <= super::ATTACHMENT_PART_DEPTH && super::is_part_id(id)
        })
        .map(str::to_string);
    Attachment {
        filename: part.filename().to_string(),
        mime_type: part.mime_type.to_string(),
        size: part.size,
        inline,
        part_id,
    }
}

/// The `charset` parameter of a `Content-Type` value, unquoted. The value
/// is cut at `;` and `=` by byte, which are ASCII and so never inside a
/// character. Each parameter is searched for `=` only up to its own end, so
/// each byte of the value is read once.
fn charset(content_type: &str) -> Option<&str> {
    let bytes = content_type.as_bytes();
    let next = |from: usize, to: usize, byte: u8| {
        bytes[from..to]
            .iter()
            .position(|&b| b == byte)
            .map(|at| from + at)
    };
    let mut start = next(0, bytes.len(), b';')? + 1;
    loop {
        let end = next(start, bytes.len(), b';').unwrap_or(bytes.len());
        if let Some(equals) = next(start, end, b'=')
            && content_type[start..equals]
                .trim()
                .eq_ignore_ascii_case("charset")
        {
            return Some(content_type[equals + 1..end].trim().trim_matches('"'));
        }
        if end == bytes.len() {
            return None;
        }
        start = end + 1;
    }
}

/// `bytes` as text in `charset`: UTF-8 (and ASCII, and no charset at all) and
/// Windows-1252 exactly; any other charset as UTF-8, with a note. Invalid
/// UTF-8 is replaced with U+FFFD, with a note.
///
/// The ISO-8859-1 labels are read as Windows-1252, as browsers read them:
/// mail so labelled routinely carries Windows-1252 punctuation in 0x80–0x9F,
/// where ISO-8859-1 has only invisible control characters.
pub fn decode_charset<'a>(
    bytes: Vec<u8>,
    charset: Option<Cow<'a, str>>,
    notes: &mut Vec<Note<'a>>,
) -> String {
    let named = |names: &[&str]| {
        charset
            .as_deref()
            .is_some_and(|charset| names.iter().any(|name| charset.eq_ignore_ascii_case(name)))
    };
    if named(&[
        "windows-1252",
        "cp1252",
        "iso-8859-1",
        "latin1",
        "iso_8859-1",
        "l1",
    ]) {
        return windows_1252(&bytes);
    }
    let unknown = charset.is_some() && !named(&["utf-8", "utf8", "us-ascii", "ascii"]);
    if unknown && let Some(charset) = charset {
        notes.push(Note::UnknownCharset(charset));
    }
    match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(error) => {
            if !unknown {
                notes.push(Note::InvalidUtf8);
            }
            lossy(error.as_bytes())
        }
    }
}

/// `bytes` in Windows-1252 as text. Each byte's character is copied from a
/// table rather than encoded, so the text is never checked again as UTF-8.
fn windows_1252(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 3);
    let mut at = 0;
    while at < bytes.len() {
        let ascii = crate::bytes::find(bytes, at, crate::bytes::high, |b| !b.is_ascii());
        text.push_str(std::str::from_utf8(&bytes[at..ascii]).expect("ASCII"));
        at = ascii;
        while let Some(&b) = bytes.get(at)
            && !b.is_ascii()
        {
            let b = usize::from(b);
            text.push_str(match b {
                0x80..=0x9f => CP1252_HIGH[b - 0x80],
                _ => &LATIN1_HIGH[(b - 0xa0) * 2..(b - 0xa0) * 2 + 2],
            });
            at += 1;
        }
    }
    text.shrink_to_fit();
    text
}

/// `bytes` as UTF-8, each invalid sequence replaced with U+FFFD, as
/// `String::from_utf8_lossy` replaces them. U+FFFD is three bytes and
/// replaces at least one, so the text fits one buffer of three times the
/// bytes, which never grows and is shrunk once written.
fn lossy(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 3);
    let mut at = 0;
    'runs: while at < bytes.len() {
        for chunk in bytes[at..].utf8_chunks() {
            text.push_str(chunk.valid());
            at += chunk.valid().len() + chunk.invalid().len();
            if chunk.invalid().is_empty() {
                continue;
            }
            // The replacements in a row are counted and copied from a block
            // of them, not written one at a time.
            let mut replaced = 1;
            while bytes.get(at).is_some_and(|b| !b.is_ascii())
                && let Err(len) = utf8_char(&bytes[at..])
            {
                replaced += 1;
                at += len;
            }
            replace(&mut text, replaced);
            if replaced > 1 {
                // The chunks resume after the replacements.
                continue 'runs;
            }
        }
    }
    text.shrink_to_fit();
    text
}

/// U+FFFD written `count` times after `text`.
fn replace(text: &mut String, mut count: usize) {
    if count == 1 {
        text.push(char::REPLACEMENT_CHARACTER);
        return;
    }
    while count > 0 {
        let block = count.min(REPLACEMENTS_HELD);
        text.push_str(&REPLACEMENTS[..block * 3]);
        count -= block;
    }
}

/// How many U+FFFD [`REPLACEMENTS`] holds.
const REPLACEMENTS_HELD: usize = 64;

/// U+FFFD [`REPLACEMENTS_HELD`] times, three bytes each.
const REPLACEMENTS: &str = {
    const BYTES: [u8; REPLACEMENTS_HELD * 3] = {
        let mut bytes = [0; REPLACEMENTS_HELD * 3];
        let mut at = 0;
        while at < bytes.len() {
            bytes[at] = 0xef;
            bytes[at + 1] = 0xbf;
            bytes[at + 2] = 0xbd;
            at += 3;
        }
        bytes
    };
    match std::str::from_utf8(&BYTES) {
        Ok(text) => text,
        Err(_) => panic!("U+FFFD is UTF-8"),
    }
};

/// The length of the UTF-8 character `bytes` starts with, or, where it
/// starts none, the length of what one U+FFFD replaces: the longest prefix
/// of a character it holds, and at least one byte. These are Unicode's
/// "maximal subparts", as `String::from_utf8_lossy` replaces them.
#[inline]
fn utf8_char(bytes: &[u8]) -> Result<usize, usize> {
    let (len, second) = match bytes[0] {
        0x00..=0x7f => return Ok(1),
        0xc2..=0xdf => (2, 0x80..=0xbf),
        0xe0 => (3, 0xa0..=0xbf),
        0xe1..=0xec | 0xee..=0xef => (3, 0x80..=0xbf),
        0xed => (3, 0x80..=0x9f),
        0xf0 => (4, 0x90..=0xbf),
        0xf1..=0xf3 => (4, 0x80..=0xbf),
        0xf4 => (4, 0x80..=0x8f),
        _ => return Err(1),
    };
    if !bytes.get(1).is_some_and(|b| second.contains(b)) {
        return Err(1);
    }
    for at in 2..len {
        if !bytes.get(at).is_some_and(|b| (0x80..=0xbf).contains(b)) {
            return Err(at);
        }
    }
    Ok(len)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_header_is_the_first_case_insensitive_match() {
        let headers = json!([
            { "name": "Received", "value": "by mx.example.com" },
            { "name": "SUBJECT", "value": "first" },
            { "name": "Subject", "value": "second" },
        ]);
        assert_eq!(header(&headers, "subject"), Ok(Some("first")));
        assert_eq!(header(&headers, "Subject"), Ok(Some("first")));
        assert_eq!(header(&headers, "Date"), Ok(None));
        assert_eq!(header(&json!([]), "Subject"), Ok(None));
    }

    #[test]
    fn a_header_list_not_of_gmails_shape_is_malformed() {
        for headers in [
            json!({}),
            json!("Subject: Lunch"),
            json!([{ "name": "From" }]),
            json!([{ "value": "nameless" }]),
            json!([{ "name": "Subject", "value": 7 }]),
            json!([{ "name": 7, "value": "Lunch" }]),
            json!([{ "name": "Subject", "value": "Lunch" }, "Date"]),
        ] {
            assert_eq!(
                header(&headers, "Subject"),
                Err(MalformedHeaders),
                "{headers}"
            );
        }
    }

    #[test]
    fn named_references_decode() {
        assert_eq!(
            decode_entities("Tom &amp; Jerry &lt;b&gt; &quot;hi&quot; it&apos;s"),
            "Tom & Jerry <b> \"hi\" it's"
        );
        assert_eq!(decode_entities("a&nbsp;b"), "a\u{a0}b");
        assert_eq!(decode_entities("co&shy;op&zwj;&zwnj;"), "coop");
    }

    #[test]
    fn every_named_reference_is_found_and_no_other_name() {
        for (name, text) in NAMED {
            assert_eq!(decode_entities(&format!("&{name};")), *text, "{name}");
            for other in [
                format!("&{name}x;"),
                format!("&x{name};"),
                format!("&{};", name.to_ascii_uppercase()),
            ] {
                if NAMED
                    .iter()
                    .all(|(known, _)| other[1..other.len() - 1] != **known)
                {
                    assert_eq!(decode_entities(&other), other, "{other}");
                }
            }
        }
        for text in ["&;", "&#;", "&#x;", "&zz;", "&ampersand;", "&nbspnbspn;"] {
            assert_eq!(decode_entities(text), text, "{text}");
        }
    }

    #[test]
    fn numeric_references_decode() {
        assert_eq!(decode_entities("it&#39;s"), "it's");
        assert_eq!(
            decode_entities("&#x2019;&#X2014;&#8364;"),
            "\u{2019}\u{2014}€"
        );
        assert_eq!(decode_entities("&#x1F600;"), "😀");
        assert_eq!(
            decode_entities("&#1114111;&#x10FFFF;"),
            "\u{10ffff}\u{10ffff}"
        );
        assert_eq!(decode_entities("&#000065;&#x000041;"), "AA");
    }

    #[test]
    fn a_numeric_reference_to_no_character_is_the_replacement_character() {
        for text in [
            "&#0;",
            "&#xD800;",
            "&#x110000;",
            "&#99999999999999999999999;",
            "&#1114112;",
            "&#x110000;",
        ] {
            assert_eq!(decode_entities(text), "\u{fffd}", "{text}");
        }
    }

    #[test]
    fn what_is_not_a_reference_is_left_verbatim() {
        for text in [
            "AT&T",
            "a & b",
            "&unknown;",
            "&amp",
            "&#;",
            "&#x;",
            "&#12a;",
            "&;",
            "trailing &",
        ] {
            assert_eq!(decode_entities(text), text, "{text}");
        }
        let long = format!("&{};", "a".repeat(40));
        assert_eq!(decode_entities(&long), long);
        assert_eq!(decode_entities("&&amp;&"), "&&&");
        assert_eq!(decode_entities("é&amp;ü"), "é&ü");
    }

    #[test]
    fn decoding_is_one_pass() {
        assert_eq!(decode_entities("&amp;lt;"), "&lt;");
    }

    #[test]
    fn internal_date_is_rfc3339_utc_to_the_second() {
        assert_eq!(internal_date("0").as_deref(), Some("1970-01-01T00:00:00Z"));
        assert_eq!(
            internal_date("1793455509123").as_deref(),
            Some("2026-10-31T14:05:09Z")
        );
        // A leap day, and the day after.
        assert_eq!(
            internal_date("1709208000000").as_deref(),
            Some("2024-02-29T12:00:00Z")
        );
        assert_eq!(
            internal_date("1709251199999").as_deref(),
            Some("2024-02-29T23:59:59Z")
        );
        assert_eq!(
            internal_date("1709251200000").as_deref(),
            Some("2024-03-01T00:00:00Z")
        );
        // 2100 is not a leap year; 2000 was.
        assert_eq!(
            internal_date("4107542400000").as_deref(),
            Some("2100-03-01T00:00:00Z")
        );
        assert_eq!(
            internal_date("951782400000").as_deref(),
            Some("2000-02-29T00:00:00Z")
        );
        // A far date.
        assert_eq!(
            internal_date("253402300799000").as_deref(),
            Some("9999-12-31T23:59:59Z")
        );
    }

    #[test]
    fn internal_date_refuses_what_is_not_epoch_milliseconds() {
        for text in [
            "",
            "-1",
            "12a",
            "1.5",
            " 1",
            "+1",
            "99999999999999999999999",
        ] {
            assert_eq!(internal_date(text), None, "{text}");
        }
    }

    #[test]
    fn base64url_decodes_either_alphabet_padded_or_not() {
        assert_eq!(base64url("").unwrap(), b"");
        assert_eq!(base64url("TWFu").unwrap(), b"Man");
        assert_eq!(base64url("TWE").unwrap(), b"Ma");
        assert_eq!(base64url("TWE=").unwrap(), b"Ma");
        assert_eq!(base64url("TQ==").unwrap(), b"M");
        assert_eq!(base64url("-_-_").unwrap(), [0xfb, 0xff, 0xbf]);
        assert_eq!(base64url("+/+/").unwrap(), [0xfb, 0xff, 0xbf]);
        assert_eq!(base64url("TW\r\nFu IA\t").unwrap(), b"Man ");
    }

    #[test]
    fn base64url_in_pieces_split_anywhere_decodes_as_whole() {
        let data = "TWFuIGlz\r\nIGRpc3Rp bmd1aXNo-_-_ZWQ=";
        let whole = base64url(data).unwrap();
        assert_eq!(whole, b"Man is distinguish\xfb\xff\xbfed");
        for split in 0..=data.len() {
            let mut decoder = Base64Url::default();
            let mut out = Vec::new();
            for piece in [&data[..split], &data[split..]] {
                assert_eq!(decoder.push(piece.as_bytes(), &mut out), piece.len());
            }
            assert!(decoder.whole());
            assert_eq!(out, whole, "split at {split}");
        }
        let mut decoder = Base64Url::default();
        assert_eq!(decoder.push(b"TWFuTWFu*TWFu", &mut Vec::new()), 8);
        assert_eq!(Base64Url::default().push(b"TWF*", &mut Vec::new()), 3);
    }

    #[test]
    fn base64url_refuses_what_is_not_base64() {
        for data in ["TW*u", "TWFuT", "é", "TWFu.", "T"] {
            assert_eq!(base64url(data), None, "{data}");
        }
    }

    #[test]
    fn three_charsets_decode_exactly() {
        let mut notes = Vec::new();
        assert_eq!(
            decode_charset(b"caf\xe9".to_vec(), Some("ISO-8859-1".into()), &mut notes),
            "café"
        );
        for latin1 in ["latin1", "iso_8859-1", "L1"] {
            assert_eq!(
                decode_charset(b"\x80\x93".to_vec(), Some(latin1.into()), &mut notes),
                "€\u{201c}"
            );
        }
        assert_eq!(
            decode_charset(
                b"\x80 \x93x\x94 \x9f \xe9".to_vec(),
                Some("Windows-1252".into()),
                &mut notes
            ),
            "€ \u{201c}x\u{201d} Ÿ é"
        );
        for undefined in [0x81, 0x8d, 0x8f, 0x90, 0x9d] {
            assert_eq!(
                decode_charset(vec![undefined], Some("cp1252".into()), &mut notes),
                "\u{fffd}"
            );
        }
        assert_eq!(CP1252_HIGH.iter().filter(|c| **c != "\u{fffd}").count(), 27);
        for (at, c) in LATIN1_HIGH.chars().enumerate() {
            assert_eq!(u32::from(c), 0xa0 + at as u32);
        }
        assert_eq!(LATIN1_HIGH.chars().count(), 96);
        for name in [None, Some("utf-8"), Some("UTF8"), Some("us-ascii")] {
            assert_eq!(
                decode_charset("é😀".as_bytes().to_vec(), name.map(Cow::from), &mut notes),
                "é😀"
            );
        }
        assert_eq!(notes, []);
    }

    #[test]
    fn invalid_utf8_is_replaced_as_the_standard_library_replaces_it() {
        for bytes in [
            &b""[..],
            b"plain",
            b"a\xffb",
            b"\xff\xff\xff",
            b"\xe2\x82",
            b"\xe2\x82\xacx\xe2",
            b"\xf0\x9f\x98",
            b"\xc0\xaf\xed\xa0\x80",
            "é😀".as_bytes(),
        ] {
            let text = lossy(bytes);
            assert_eq!(text, String::from_utf8_lossy(bytes), "{bytes:?}");
            assert!(text.len() <= 3 * bytes.len());
        }
        // Every sequence of two bytes, and of three and four from the bytes
        // at the edges of each range a character's bytes are checked
        // against, each between ASCII long enough to be read a word at a
        // time.
        let edges = [
            0x00, 0x7f, 0x80, 0x8f, 0x90, 0x9f, 0xa0, 0xbf, 0xc0, 0xc2, 0xe0, 0xed,
        ];
        let edges = edges.into_iter().chain([0xee, 0xf0, 0xf4, 0xf5, 0xff]);
        let mut sequences: Vec<Vec<u8>> = Vec::new();
        for first in 0..=0xff {
            for second in 0..=0xff {
                sequences.push(vec![first, second]);
            }
            for second in edges.clone() {
                for third in edges.clone() {
                    sequences.push(vec![first, second, third]);
                    for fourth in [0x80, 0xbf, 0xc0] {
                        sequences.push(vec![first, second, third, fourth]);
                    }
                }
            }
        }
        for sequence in sequences {
            let bytes = [&b"abcdefgh"[..], &sequence, b"ijklmnop"].concat();
            assert_eq!(
                lossy(&bytes),
                String::from_utf8_lossy(&bytes),
                "{sequence:x?}"
            );
        }
    }

    #[test]
    fn runs_of_replacements_are_replaced_as_the_standard_library_replaces_them() {
        let units: [&[u8]; 6] = [
            b"\xff",
            b"\xc2",
            b"\xffa",
            b"\xff\xc3\xa9",
            b"\xe0\xa0",
            b"\xff\xffab",
        ];
        for unit in units {
            for n in [1, 2, 63, 64, 65, 128, 129, 300] {
                let bytes = unit.repeat(n);
                assert_eq!(
                    lossy(&bytes),
                    String::from_utf8_lossy(&bytes),
                    "{unit:x?} {n}"
                );
                let framed = [&b"<p>"[..], &bytes, b"</p>"].concat();
                assert_eq!(
                    lossy(&framed),
                    String::from_utf8_lossy(&framed),
                    "{unit:x?} {n}"
                );
            }
        }
    }

    #[test]
    fn windows_1252_decodes_each_byte_alone_and_in_runs() {
        let every: Vec<u8> = (0..=0xff).collect();
        let one_by_one: String = every.iter().map(|&b| windows_1252(&[b])).collect();
        assert_eq!(windows_1252(&every), one_by_one);
        for b in 0..=0xffu8 {
            let expected = match b {
                0x80..=0x9f => CP1252_HIGH[usize::from(b - 0x80)].to_string(),
                _ => char::from(b).to_string(),
            };
            assert_eq!(windows_1252(&[b]), expected, "{b:#x}");
        }
    }

    #[test]
    fn invalid_utf8_and_unknown_charsets_are_noted() {
        let mut notes = Vec::new();
        assert_eq!(
            decode_charset(b"a\xffb".to_vec(), None, &mut notes),
            "a\u{fffd}b"
        );
        assert_eq!(notes, [Note::InvalidUtf8]);
        let mut notes = Vec::new();
        assert_eq!(
            decode_charset(b"ok".to_vec(), Some("KOI8-R".into()), &mut notes),
            "ok"
        );
        assert_eq!(notes, [Note::UnknownCharset("KOI8-R".into())]);
        // An unknown charset's invalid bytes are replaced under its own note
        // alone, and the known names are matched in any case.
        let mut notes = Vec::new();
        assert_eq!(
            decode_charset(b"a\xffb".to_vec(), Some("KOI8-R".into()), &mut notes),
            "a\u{fffd}b"
        );
        assert_eq!(notes, [Note::UnknownCharset("KOI8-R".into())]);
        for name in ["Utf-8", "US-ASCII", "Windows-1252", "CP1252", "L1"] {
            let mut notes = Vec::new();
            decode_charset(b"ok".to_vec(), Some(name.into()), &mut notes);
            assert_eq!(notes, [], "{name}");
        }
    }

    #[test]
    fn the_charset_parameter_is_read_case_insensitively_and_unquoted() {
        assert_eq!(charset("text/plain; CharSet=\"UTF-8\""), Some("UTF-8"));
        assert_eq!(
            charset("text/plain; format=flowed; charset=iso-8859-1"),
            Some("iso-8859-1")
        );
        assert_eq!(charset("text/plain"), None);
        assert_eq!(charset("charset=utf-8"), None);
        // Read as `split` reads it, on every way the parameters can fall.
        let split = |content_type: &str| -> Option<String> {
            content_type.split(';').skip(1).find_map(|param| {
                let (key, value) = param.split_once('=')?;
                key.trim()
                    .eq_ignore_ascii_case("charset")
                    .then(|| value.trim().trim_matches('"').to_string())
            })
        };
        for content_type in [
            "text/plain;",
            "text/plain; charset",
            "text/plain; charset; charset=a",
            "text/plain; format; x=y; CHARSET = \"\"b\"\" ",
            "text/plain; x=a=b; charset=c=d",
            "text/plain;charset=é;charset=f",
            "text/plain; =; charset=",
            "text/plain;\u{a0}charset\u{a0}=\u{a0}g\u{a0}",
            ";;;charset=h;",
        ] {
            assert_eq!(
                charset(content_type).map(str::to_string),
                split(content_type),
                "{content_type:?}"
            );
        }
        // Many parameters without `=` before the charset, and none after.
        let padded = format!("text/html{}; charset=utf-8", ";".repeat(100_000));
        assert_eq!(charset(&padded), Some("utf-8"));
        assert_eq!(charset(&";".repeat(100_000)), None);
    }

    /// `note` holding its strings.
    fn owned(note: Note<'_>) -> Note<'static> {
        match note {
            Note::UnknownCharset(name) => Note::UnknownCharset(name.into_owned().into()),
            Note::InvalidUtf8 => Note::InvalidUtf8,
            Note::UnlistedChildren { mime_type, depth } => Note::UnlistedChildren {
                mime_type: mime_type.into_owned().into(),
                depth,
            },
        }
    }

    /// `payload` read as a part tree and walked by [`extract`].
    fn extracted(payload: &Value) -> Result<Extracted<'static>, WalkError> {
        let extracted = extract(&Part::from_value(payload)?)?;
        Ok(Extracted {
            notes: extracted.notes.into_iter().map(owned).collect(),
            ..extracted
        })
    }

    /// A `multipart/alternative` payload of `children`.
    fn alternative(children: Value) -> Value {
        json!({ "mimeType": "multipart/alternative", "parts": children })
    }

    fn html_part() -> Value {
        // "<p>hi</p>"
        json!({ "mimeType": "text/html", "body": { "data": "PHA-aGk8L3A-" } })
    }

    fn plain_part(data: &str, charset: &str) -> Value {
        json!({ "mimeType": "text/plain", "body": { "data": data },
                "headers": [{ "name": "Content-Type",
                              "value": format!("text/plain; charset={charset}") }] })
    }

    #[test]
    fn an_alternative_prefers_plain_wherever_it_stands() {
        // "plain"
        let read = extracted(&alternative(json!([
            html_part(),
            plain_part("cGxhaW4", "utf-8")
        ])))
        .unwrap();
        assert_eq!(read.text_parts, [("plain".to_string(), Source::Plain)]);
        assert_eq!(read.notes, []);
    }

    #[test]
    fn an_alternative_without_plain_takes_html_and_lists_the_rest() {
        let image = json!({ "mimeType": "image/png", "body": { "size": 4 } });
        let read = extracted(&alternative(json!([image, html_part()]))).unwrap();
        assert_eq!(read.text_parts, [("hi".to_string(), Source::Html)]);
        assert_eq!(read.notes, []);
        assert_eq!(
            read.attachments,
            [Attachment {
                filename: String::new(),
                mime_type: "image/png".to_string(),
                size: Some(4),
                inline: false,
                part_id: None,
            }]
        );
    }

    /// `leaf` wrapped in `levels` levels of `multipart/mixed`, so it sits at
    /// depth `levels`.
    fn nested(leaf: Value, levels: usize) -> Value {
        (0..levels).fold(leaf, |inner, level| {
            json!({ "partId": format!("w{level}"), "mimeType": "multipart/mixed",
                    "parts": [inner] })
        })
    }

    fn served(part_id: Value) -> Value {
        json!({ "partId": part_id, "mimeType": "application/pdf", "filename": "a.pdf",
                "body": { "size": 3, "attachmentId": "ATT" } })
    }

    #[test]
    fn a_part_id_is_given_to_a_served_attachment_within_the_fetch_depth() {
        let part_ids = |payload: &Value| -> Vec<Option<String>> {
            extracted(payload)
                .unwrap()
                .attachments
                .into_iter()
                .map(|a| a.part_id)
                .collect()
        };
        let depth = crate::gmail::ATTACHMENT_PART_DEPTH;
        assert_eq!(
            part_ids(&nested(served(json!("1.0.2")), depth)),
            [Some("1.0.2".to_string())]
        );
        assert_eq!(part_ids(&served(json!("0"))), [Some("0".to_string())]);
        // One level deeper than the fetch reads: listed, unnamed.
        assert_eq!(part_ids(&nested(served(json!("1.0.2")), depth + 1)), [None]);
        // Carried inside the message: no attachmentId, no name.
        let carried = json!({ "partId": "1", "mimeType": "image/png", "filename": "a.png",
                              "body": { "size": 3, "data": "YWJj" } });
        assert_eq!(part_ids(&carried), [None]);
        // No partId, an empty one, or one not of the part-id grammar:
        // unnamed. One that is not a string is not of Gmail's shape.
        let mut bare = served(json!("x"));
        bare.as_object_mut().unwrap().remove("partId");
        assert_eq!(part_ids(&bare), [None]);
        for id in [json!(""), json!("1/2"), json!("1".repeat(65))] {
            assert_eq!(part_ids(&served(id.clone())), [None], "{id}");
        }
        assert_eq!(extracted(&served(json!(3))), Err(WalkError::Shape));
        assert_eq!(
            part_ids(&served(json!("1".repeat(64)))),
            [Some("1".repeat(64))]
        );
    }

    #[test]
    fn a_blank_plain_alternative_gives_way_to_html() {
        // "\r\n" and " \t"
        for blank in ["DQo", "IAk"] {
            let read = extracted(&alternative(json!([
                plain_part(blank, "utf-8"),
                html_part()
            ])))
            .unwrap();
            assert_eq!(read.text_parts, [("hi".to_string(), Source::Html)]);
        }
        // "<p> </p>": nothing yields text, so the first child is read.
        let blank_html = json!({ "mimeType": "text/html", "body": { "data": "PHA-IDwvcD4" } });
        let read = extracted(&alternative(json!([
            plain_part("DQo", "utf-8"),
            blank_html
        ])))
        .unwrap();
        assert_eq!(read.text_parts, [("\r\n".to_string(), Source::Plain)]);
    }

    #[test]
    fn an_alternative_not_read_keeps_only_its_unlisted_children_note() {
        let read = extracted(&alternative(json!([
            plain_part("cGxhaW4", "utf-8"),
            plain_part("b3RoZXI", "KOI8-R"),
            html_part(),
            { "mimeType": "multipart/related" },
        ])))
        .unwrap();
        assert_eq!(read.text_parts, [("plain".to_string(), Source::Plain)]);
        assert_eq!(
            read.notes,
            [Note::UnlistedChildren {
                mime_type: "multipart/related".into(),
                depth: 1,
            }]
        );
    }

    #[test]
    fn either_inline_signal_alone_marks_an_attachment_inline() {
        let part = |headers: Value| {
            json!({ "mimeType": "image/png", "filename": "a.png",
                    "body": { "attachmentId": "A" }, "headers": headers })
        };
        let read = extracted(&json!({ "mimeType": "multipart/mixed", "parts": [
            part(json!([{ "name": "Content-ID", "value": "<a@example.com>" },
                        { "name": "Content-Disposition", "value": "attachment" }])),
            part(json!([{ "name": "content-disposition", "value": " INLINE; filename=a.png" }])),
            part(json!([{ "name": "Content-Disposition", "value": "attachment; filename=a.png" }])),
            part(json!([])),
        ] }))
        .unwrap();
        let inline: Vec<bool> = read.attachments.iter().map(|a| a.inline).collect();
        assert_eq!(inline, [true, true, false, false]);
    }

    #[test]
    fn an_html_part_is_decoded_in_its_charset_then_read_as_text() {
        // "<p>caf\xe9 \x93hi\x94</p>" in Windows-1252.
        let part = json!({ "mimeType": "text/html", "body": { "data": "PHA-Y2Fm6SCTaGmUPC9wPg" },
                           "headers": [{ "name": "Content-Type",
                                         "value": "text/html; charset=windows-1252" }] });
        let read = extracted(&part).unwrap();
        assert_eq!(
            read.text_parts,
            [("café \u{201c}hi\u{201d}".to_string(), Source::Html)]
        );
        assert_eq!(read.notes, []);
    }

    #[test]
    fn an_html_part_with_no_text_yields_an_empty_html_text() {
        // "<img src=\"cid:a\">"
        let part =
            json!({ "mimeType": "text/html", "body": { "data": "PGltZyBzcmM9ImNpZDphIj4" } });
        let read = extracted(&part).unwrap();
        assert_eq!(read.text_parts, [(String::new(), Source::Html)]);
    }

    #[test]
    fn tags_are_removed_and_links_keep_their_text() {
        assert_eq!(
            html_to_text(
                "<span class=\"x\">Hello <b>there</b>, <a href=\"https://example.com/t?u=1&amp;v=2\">read more</a>.</span>"
            ),
            "Hello there, read more."
        );
        assert_eq!(html_to_text("<IMG SRC=\"a.png\" ALT=\"logo\">Text"), "Text");
        assert_eq!(
            html_to_text("<a title=\"a > b\" href='x>y'>link</a> after"),
            "link after"
        );
        assert_eq!(html_to_text("<!DOCTYPE html><?xml version=\"1.0\"?>x"), "x");
    }

    #[test]
    fn a_lone_angle_bracket_is_text() {
        assert_eq!(html_to_text("a < b and c<5 <3"), "a < b and c<5 <3");
        assert_eq!(html_to_text("x<"), "x<");
    }

    #[test]
    fn line_and_paragraph_tags_break_the_text() {
        assert_eq!(html_to_text("one<br>two<BR/>three"), "one\ntwo\nthree");
        assert_eq!(
            html_to_text("<ul><li>a</li><li>b</li></ul>after"),
            "a\nb\n\nafter"
        );
        for tag in [
            "address",
            "article",
            "aside",
            "blockquote",
            "caption",
            "center",
            "details",
            "dialog",
            "dir",
            "div",
            "dl",
            "fieldset",
            "figcaption",
            "figure",
            "footer",
            "form",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
            "header",
            "hgroup",
            "legend",
            "listing",
            "main",
            "menu",
            "nav",
            "ol",
            "p",
            "plaintext",
            "pre",
            "search",
            "section",
            "summary",
            "table",
            "tr",
            "ul",
            "xmp",
        ] {
            assert_eq!(
                html_to_text(&format!("a<{tag}>b</{tag}>c")),
                "a\n\nb\n\nc",
                "{tag}"
            );
        }
        assert_eq!(html_to_text("a<hr>b"), "a\n\nb");
        assert_eq!(html_to_text("a<span>b</span>c<em>d</em>"), "abcd");
        assert_eq!(
            html_to_text("<dl><dt>Name</dt><dd>Bob</dd><dt>Total</dt><dd>$40.00</dd></dl>after"),
            "Name\nBob\nTotal\n$40.00\n\nafter"
        );
    }

    #[test]
    fn table_cells_are_tab_separated() {
        assert_eq!(
            html_to_text(
                "<table><tr><th>Item</th><th>Price</th></tr>\n<tr><td> Tea </td><td>&euro;3</td></tr></table>"
            ),
            "Item\tPrice\n\nTea\t€3"
        );
    }

    #[test]
    fn comments_scripts_styles_and_the_heads_content_are_dropped() {
        assert_eq!(
            html_to_text(
                "<html><HEAD><title>T</title><style>p{color:red}</style></head>\
                 <body><!-- hidden <p>x</p> -->Shown<script type=\"x\">if (a<b) {}</SCRIPT>\
                 <style>.a{}</style> text</body></html>"
            ),
            "Shown text"
        );
        // `</head>` omitted, with and without `<body>`.
        assert_eq!(
            html_to_text("<head><title>T</title><body>Shown</body>"),
            "Shown"
        );
        assert_eq!(
            html_to_text("<head><meta charset=utf-8><link rel=x href=y><p>Hello"),
            "Hello"
        );
    }

    #[test]
    fn a_quote_opens_a_value_only_after_equals() {
        assert_eq!(
            html_to_text("<img alt=Bob's src=x.png>Hello there"),
            "Hello there"
        );
        assert_eq!(
            html_to_text("<p class=a'b>Hi</p> Don't stop <b>here</b>."),
            "Hi\n\nDon't stop here."
        );
        assert_eq!(html_to_text("<a title = 'x > y'>t</a>"), "t");
        assert_eq!(html_to_text("<a data-x\"y>t</a>"), "t");
    }

    #[test]
    fn comments_end_where_html_ends_them() {
        assert_eq!(
            html_to_text("<!-->shown after empty comment"),
            "shown after empty comment"
        );
        assert_eq!(html_to_text("<p>a</p><!--->b"), "a\n\nb");
        assert_eq!(html_to_text("a<!---->b"), "ab");
        assert_eq!(html_to_text("x<!-- c --!>y"), "xy");
        assert_eq!(html_to_text("x<!-- c --!>y<!-- d -->z"), "xyz");
        assert_eq!(html_to_text("x<!-- c -->y --!>z"), "xy --!>z");
        assert_eq!(html_to_text("x<!-- a - b -- c --->y"), "xy");
    }

    /// `html` read as text in two passes: the text written with a newline or
    /// two for each break, a tab for each cell and one space for each
    /// whitespace run, then each line trimmed and blank runs kept to one.
    fn two_pass(html: &str) -> String {
        let (mut raw, mut space) = (String::new(), false);
        let text = |raw: &mut String, space: &mut bool, run: &str| {
            let decoded = decode_entities(run);
            for (at, word) in decoded.split(|c: char| c.is_ascii_whitespace()).enumerate() {
                *space |= at > 0;
                if word.is_empty() {
                    continue;
                }
                if *space && !matches!(raw.as_bytes().last(), None | Some(b'\n' | b'\t')) {
                    raw.push(' ');
                }
                *space = false;
                raw.push_str(word);
            }
        };
        let (mut from, mut at) = (0, 0);
        while let Some(found) = html[at..].find('<') {
            let open = at + found;
            let next = html.as_bytes().get(open + 1);
            if !next.is_some_and(|b| b.is_ascii_alphabetic() || matches!(b, b'/' | b'!' | b'?')) {
                at = open + 1;
                continue;
            }
            text(&mut raw, &mut space, &html[from..open]);
            let end = if html[open..].starts_with("<!--") {
                comment_end(html, open + 4)
            } else {
                tag_end(html.as_bytes(), open + 1).and_then(|end| {
                    let inner = &html[open + 1..end];
                    let (inner, close) = match inner.strip_prefix('/') {
                        Some(rest) => (rest, true),
                        None => (inner, false),
                    };
                    let len = inner
                        .bytes()
                        .position(|b| !b.is_ascii_alphanumeric())
                        .unwrap_or(inner.len());
                    let name = &inner[..len];
                    let kind = kind_of(name);
                    if !close && kind == Some(Kind::Dropped) {
                        return dropped_end(html.as_bytes(), name.as_bytes(), end);
                    }
                    let mark = match tag_break(kind, close) {
                        Break::None => "",
                        Break::Line => "\n",
                        Break::Paragraph => "\n\n",
                        Break::Tab => "\t",
                    };
                    if !mark.is_empty() {
                        raw.push_str(mark);
                        space = false;
                    }
                    Some(end)
                })
            };
            let Some(end) = end else {
                from = html.len();
                break;
            };
            (from, at) = (end, end);
        }
        text(&mut raw, &mut space, &html[from..]);
        let (mut out, mut blank) = (String::new(), false);
        for line in raw.split('\n').map(str::trim) {
            if line.is_empty() {
                blank = !out.is_empty();
                continue;
            }
            if !out.is_empty() {
                out.push_str(if blank { "\n\n" } else { "\n" });
            }
            blank = false;
            out.push_str(line);
        }
        out
    }

    #[test]
    fn lines_are_trimmed_and_blank_runs_kept_to_one_as_they_are_written() {
        const PIECES: [&str; 22] = [
            "a", "é", "😀", " ", "\n", "\u{a0}", "\u{b}", "\u{2003}", "&nbsp;", "&#11;", "&#32;",
            "&#10;", "&shy;", "&amp;", "&", "<p>", "</p>", "<br>", "<td>", "<b>", "<!--c-->", "<",
        ];
        let mut html = String::new();
        let mut count = [0; 4];
        'cases: loop {
            html.clear();
            for &piece in &count {
                html.push_str(PIECES[piece]);
            }
            assert_eq!(html_to_text(&html), two_pass(&html), "{html:?}");
            for digit in &mut count {
                *digit += 1;
                if *digit < PIECES.len() {
                    continue 'cases;
                }
                *digit = 0;
            }
            break;
        }
        for html in [
            "\n\n\ta \n\n\n b\t\n",
            "<p>\u{a0}a\u{a0}</p>\u{2002}<br>\u{3000}b",
            "\t\t<td><td>a<br>b<p><p><p>c",
            "&#11;a&#11;<br>\u{85}\u{1c}b\u{85}<br>&nbsp;&nbsp;",
            "\u{1f600}&emsp;<br>&emsp;\u{1f600}",
            "<td> Tea </td><td>&euro;3 <td> </td>\u{a0}<br>",
        ] {
            assert_eq!(html_to_text(html), two_pass(html), "{html:?}");
        }
    }

    #[test]
    fn long_runs_of_what_costs_most_render_as_short_ones_do() {
        let n = 20_000;
        assert_eq!(html_to_text(&"<!--a-->b".repeat(n)), "b".repeat(n));
        assert_eq!(html_to_text(&"<!--a--!>b".repeat(n)), "b".repeat(n));
        assert_eq!(html_to_text(&"&".repeat(n)), "&".repeat(n));
        assert_eq!(html_to_text(&"&a".repeat(n)), "&a".repeat(n));
        assert_eq!(html_to_text(&"&#128512;".repeat(n)), "\u{1f600}".repeat(n));
        assert_eq!(html_to_text(&"<P>a".repeat(n)), vec!["a"; n].join("\n\n"));
        assert_eq!(
            html_to_text(&format!("a<style>{}</STYLE>b", "</p>".repeat(n))),
            "ab"
        );
        assert_eq!(html_to_text(&"a \t\n".repeat(n)), vec!["a"; n].join(" "));
    }

    #[test]
    fn a_reference_body_ends_at_its_first_character_no_name_holds() {
        for text in ["&am p;", "&#1 2;", "&amp-;", "&a\u{e9};"] {
            assert_eq!(decode_entities(text), text, "{text}");
        }
        assert_eq!(decode_entities("&&&amp;"), "&&&");
        assert_eq!(decode_entities("&a&amp;"), "&a&");
    }

    /// What the element named `name` does, as [`tag_kind`] reads it.
    fn kind_of(name: &str) -> Option<Kind> {
        tag_kind(name.as_bytes(), name_key(name.as_bytes(), true))
    }

    #[test]
    fn a_tag_names_key_is_read_with_its_end_in_one_scan() {
        for (name, _) in TAGS {
            for written in [name.to_string(), name.to_ascii_uppercase()] {
                let source = format!("<{written} a=\"b\">");
                assert_eq!(
                    tag_name(source.as_bytes(), 1),
                    (1 + name.len(), name_key(name.as_bytes(), true)),
                    "{written}"
                );
            }
        }
        assert_eq!(tag_name(b"<blockquote>", 1), (11, 0));
        assert_eq!(tag_name(b"</p>", 2), (3, name_key(b"p", true)));
        assert_eq!(tag_name(b"<!doctype>", 1), (1, 0));
        assert_eq!(tag_name(b"<p", 1), (2, name_key(b"p", true)));
    }

    #[test]
    fn a_tag_name_is_read_in_any_case_and_a_long_one_is_unknown() {
        assert_eq!(kind_of("TaBlE"), Some(Kind::Paragraph));
        assert_eq!(kind_of("tables"), None);
        assert_eq!(kind_of("BlockQuote"), Some(Kind::Paragraph));
        assert_eq!(kind_of("blockquotes"), None);
        assert_eq!(kind_of("SCRIPT"), Some(Kind::Dropped));
        assert_eq!(kind_of(""), None);
        assert_eq!(kind_of(&"p".repeat(40)), None);
        for (name, kind) in TAGS {
            assert_eq!(kind_of(name), Some(kind), "{name}");
            assert_eq!(kind_of(&name.to_ascii_uppercase()), Some(kind), "{name}");
            assert_eq!(kind_of(&format!("{name}x")), None, "{name}");
        }
        for p in ["address", "blockquote", "figcaption", "h6", "p", "xmp"] {
            assert_eq!(tag_break(kind_of(p), true), Break::Paragraph, "{p}");
        }
        assert_eq!(tag_break(kind_of("br"), false), Break::Line);
        assert_eq!(tag_break(kind_of("br"), true), Break::None);
        assert_eq!(tag_break(kind_of("th"), false), Break::Tab);
        assert_eq!(tag_break(kind_of("th"), true), Break::None);
        assert_eq!(tag_break(kind_of("tbody"), false), Break::None);
        assert_eq!(html_to_text("a<BlockQuote>b"), "a\n\nb");
        assert_eq!(html_to_text("a<FIGCAPTION>b"), "a\n\nb");
        assert_eq!(html_to_text("a<blockquotes>b"), "ab");
        assert_eq!(html_to_text("a<ScRiPt>x</sCrIpT>b"), "ab");
    }

    #[test]
    fn what_is_left_open_runs_to_the_end() {
        assert_eq!(html_to_text("a<!-- never closed <p>b"), "a");
        assert_eq!(html_to_text("a<script>b"), "a");
        assert_eq!(html_to_text("a<b class=\"x"), "a");
        assert_eq!(html_to_text("a<head><title>t"), "a");
    }

    #[test]
    fn entities_decode_after_tags_are_removed() {
        assert_eq!(
            html_to_text("&lt;b&gt;not bold&lt;/b&gt; &amp; Caf&eacute; &mdash; &copy;&nbsp;2026"),
            "<b>not bold</b> & Café \u{2014} ©\u{a0}2026"
        );
    }

    #[test]
    fn whitespace_collapses_lines_trim_and_blank_lines_are_at_most_one() {
        assert_eq!(
            html_to_text(
                "  <p>\n  Hello\t\r\n   world  </p>\n\n<p></p><p></p><div>  next </div>  "
            ),
            "Hello world\n\nnext"
        );
        assert_eq!(html_to_text("a&#32;&#32;&#10;b"), "a b");
        assert_eq!(html_to_text("<pre>  keep\n   this </pre>"), "keep this");
        assert_eq!(html_to_text(""), "");
        assert_eq!(html_to_text("<p> </p>"), "");
    }

    #[test]
    fn the_named_table_decodes_common_entities_case_sensitively() {
        assert_eq!(
            decode_entities(
                "&ndash;&mdash;&lsquo;&rsquo;&ldquo;&rdquo;&hellip;&euro;&pound;&yen;&cent;\
                 &copy;&reg;&trade;&middot;&bull;&deg;&laquo;&raquo;&times;&frac12;"
            ),
            "\u{2013}\u{2014}\u{2018}\u{2019}\u{201c}\u{201d}\u{2026}€£¥¢©®™·•°«»×½"
        );
        assert_eq!(decode_entities("&dagger;&Dagger;"), "\u{2020}\u{2021}");
        assert_eq!(decode_entities("&eacute;&Eacute;"), "éÉ");
        assert_eq!(decode_entities("&EURO;&Copy;"), "&EURO;&Copy;");
        for (name, text) in NAMED {
            assert!(name.len() <= REFERENCE_MAX_CHARS, "{name}");
            assert_eq!(decode_entities(&format!("&{name};")), *text, "{name}");
        }
    }

    /// `bytes` in base64url without padding.
    fn encode(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let group = chunk
                .iter()
                .enumerate()
                .fold(0u32, |bits, (i, b)| bits | u32::from(*b) << (16 - 8 * i));
            for i in 0..=chunk.len() {
                out.push(char::from(ALPHABET[(group >> (18 - 6 * i) & 63) as usize]));
            }
        }
        out
    }

    /// A text leaf of `mime_type` holding `bytes` in `charset`.
    fn leaf(mime_type: &str, bytes: &[u8], charset: &str) -> Value {
        json!({ "mimeType": mime_type, "body": { "data": encode(bytes) },
                "headers": [{ "name": "Content-Type",
                              "value": format!("{mime_type}; charset={charset}") }] })
    }

    fn container(mime_type: &str, children: Value) -> Value {
        json!({ "mimeType": mime_type, "parts": children })
    }

    const ALL: Wanted = Wanted {
        text: true,
        html: true,
        rendered: true,
    };

    /// What [`bodies`] answers for `payload`, its notes holding their
    /// strings.
    struct Read {
        bodies: Bodies,
        notes: Vec<Note<'static>>,
    }

    impl std::ops::Deref for Read {
        type Target = Bodies;
        fn deref(&self) -> &Bodies {
            &self.bodies
        }
    }

    fn read_bodies(payload: &Value, wanted: Wanted) -> Read {
        let (bodies, notes) = bodies(&Part::from_value(payload).unwrap(), wanted).unwrap();
        Read {
            bodies,
            notes: notes.into_iter().map(owned).collect(),
        }
    }

    /// The texts a view holds: a plain leaf's text, an HTML leaf's markup
    /// (`rendering` false) or rendering (`rendering` true).
    fn texts(read: &Bodies, view: &[usize], rendering: bool) -> Vec<String> {
        view.iter()
            .map(|&i| {
                let leaf = &read.leaves[i];
                match (leaf.source, rendering) {
                    (Source::Html, true) => leaf.rendering.clone(),
                    _ => leaf.decoded.clone(),
                }
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn a_part_keeps_the_first_of_each_header_it_reads_and_nothing_else() {
        let value = json!({
            "mimeType": "Text/HTML",
            "filename": "",
            "partId": "1.2",
            "extra": { "nested": [1, 2, "three"] },
            "headers": [
                { "name": "Received", "value": "by mx.example.com" },
                { "name": "content-type", "value": "text/html; charset=utf-8" },
                { "name": "Content-Type", "value": "text/plain" },
                { "name": "CONTENT-DISPOSITION", "value": "inline", "extra": 1 },
                { "name": "Content-Id", "value": "<a@example.com>" },
                { "name": "Content-ID", "value": "<b@example.com>" },
            ],
            "body": { "size": 9, "data": "PHA-aGk8L3A-", "attachmentId": "ATT", "other": null },
            "parts": [{ "mimeType": "text/plain" }],
        });
        let part = Part::from_value(&value).unwrap();
        assert_eq!(
            part,
            Part {
                mime_type: "Text/HTML".into(),
                data: Some("PHA-aGk8L3A-".into()),
                parts: Some(Box::new([Part {
                    mime_type: "text/plain".into(),
                    ..Part::default()
                }])),
                size: Some(9),
                meta: Some(Box::new(Meta {
                    filename: "".into(),
                    part_id: Some("1.2".into()),
                    headers: Headers {
                        content_type: Some("text/html; charset=utf-8".into()),
                        content_disposition: Some("inline".into()),
                        content_id: Some("<a@example.com>".into()),
                    },
                })),
                attachment_id: true,
            }
        );
        // Escaped strings read as their text.
        let escaped: Value =
            serde_json::from_str(r#"{ "mimeType": "text\/plain", "headers": [{ "name": "Content-Type", "value": "a\"b" }] }"#)
                .unwrap();
        let part = Part::from_value(&escaped).unwrap();
        assert_eq!(&*part.mime_type, "text/plain");
        assert_eq!(part.headers().content_type.as_deref(), Some("a\"b"));
        let part: Part = serde_json::from_str(
            r#"{ "mimeType": "text\/plain", "headers": [{ "name": "Content-Type", "value": "a\"b" }] }"#,
        )
        .unwrap();
        assert_eq!(part.headers().content_type.as_deref(), Some("a\"b"));
    }

    #[test]
    fn a_part_with_a_mistyped_field_is_not_of_gmails_shape() {
        for payload in [
            json!({}),
            json!([]),
            json!(["text/plain"]),
            json!({ "mimeType": 7 }),
            json!({ "mimeType": null }),
            json!({ "mimeType": "text/plain", "filename": 7 }),
            json!({ "mimeType": "text/plain", "filename": null }),
            json!({ "mimeType": "text/plain", "partId": 3 }),
            json!({ "mimeType": "text/plain", "headers": {} }),
            json!({ "mimeType": "text/plain", "headers": ["Subject"] }),
            json!({ "mimeType": "text/plain", "headers": [["Subject", "Lunch"]] }),
            json!({ "mimeType": "text/plain", "headers": [{ "name": "Subject" }] }),
            json!({ "mimeType": "text/plain", "headers": [{ "value": "Lunch" }] }),
            json!({ "mimeType": "text/plain", "headers": [{ "name": "X", "value": 7 }] }),
            json!({ "mimeType": "text/plain", "body": [] }),
            json!({ "mimeType": "text/plain", "body": null }),
            json!({ "mimeType": "text/plain", "body": { "attachmentId": 7 } }),
            json!({ "mimeType": "text/plain", "body": { "size": "3" } }),
            json!({ "mimeType": "text/plain", "body": { "size": -1 } }),
            json!({ "mimeType": "text/plain", "body": { "size": 1.5 } }),
            json!({ "mimeType": "text/plain", "body": { "data": 7 } }),
            json!({ "mimeType": "multipart/mixed", "parts": {} }),
            json!({ "mimeType": "multipart/mixed", "parts": ["text/plain"] }),
            json!({ "mimeType": "multipart/mixed", "parts": [{ "mimeType": 1 }] }),
        ] {
            assert_eq!(
                Part::from_value(&payload),
                Err(WalkError::Shape),
                "{payload}"
            );
        }
        for text in [
            r#"{ "mimeType": "text/plain", "mimeType": "text/html" }"#,
            r#"{ "mimeType": "text/plain", "body": { "data": "", "data": "" } }"#,
            r#"{ "mimeType": "text/plain", "headers": [{ "name": "a", "name": "b", "value": "" }] }"#,
        ] {
            assert!(serde_json::from_str::<Part>(text).is_err(), "{text}");
        }
    }

    #[test]
    fn a_message_keeps_its_id_and_payload_and_nothing_else() {
        let answer = json!({
            "id": "m1",
            "threadId": "t1",
            "snippet": "Lunch?",
            "payload": { "mimeType": "text/plain", "body": { "data": encode(b"hi") } },
        });
        let text = answer.to_string();
        let message = Message::from_slice(text.as_bytes()).unwrap();
        assert_eq!(message.id, "m1");
        assert_eq!(
            message.payload,
            Part::from_value(&answer["payload"]).unwrap()
        );
        for text in [
            "",
            "not json",
            "[]",
            r#"{ "id": "m1" }"#,
            r#"{ "payload": { "mimeType": "text/plain" } }"#,
            r#"{ "id": 1, "payload": { "mimeType": "text/plain" } }"#,
            r#"{ "id": "m1", "payload": [] }"#,
            r#"{ "id": "m1", "payload": { "mimeType": 1 } }"#,
            r#"{ "id": "m1", "id": "m2", "payload": { "mimeType": "text/plain" } }"#,
            r#"{ "id": "m1", "payload": { "mimeType": "text/plain" } } x"#,
        ] {
            assert_eq!(
                Message::from_slice(text.as_bytes()),
                Err(WalkError::Shape),
                "{text}"
            );
        }
    }

    #[test]
    fn a_plain_message_has_text_and_no_html_view() {
        let read = read_bodies(&leaf("text/plain", b"hello", "utf-8"), ALL);
        assert_eq!(texts(&read, &read.text, false), ["hello"]);
        assert!(read.html.is_empty());
        assert_eq!(read.notes, []);
    }

    #[test]
    fn an_html_only_message_renders_its_leaf_once_for_text_and_rendered() {
        let read = read_bodies(&leaf("text/html", b"<p>caf&eacute;</p>", "utf-8"), ALL);
        assert_eq!(read.text, [0]);
        assert_eq!(read.html, [0]);
        assert_eq!(
            read.leaves,
            [Leaf {
                source: Source::Html,
                decoded: Some("<p>caf&eacute;</p>".to_string()),
                rendering: Some("café".to_string()),
            }]
        );
    }

    #[test]
    fn an_alternative_reads_plain_for_text_and_html_for_html() {
        let read = read_bodies(
            &container(
                "multipart/alternative",
                json!([
                    leaf("text/plain", b"plain", "utf-8"),
                    leaf("text/html", b"<b>rich</b>", "utf-8"),
                ]),
            ),
            ALL,
        );
        assert_eq!(texts(&read, &read.text, true), ["plain"]);
        assert_eq!(texts(&read, &read.html, false), ["<b>rich</b>"]);
        assert_eq!(texts(&read, &read.html, true), ["rich"]);
    }

    #[test]
    fn a_blank_plain_alternative_gives_text_to_the_html_rendering() {
        let read = read_bodies(
            &container(
                "multipart/alternative",
                json!([
                    leaf("text/plain", b" \r\n", "utf-8"),
                    leaf("text/html", b"<p>hi</p>", "utf-8"),
                ]),
            ),
            ALL,
        );
        assert_eq!(texts(&read, &read.text, true), ["hi"]);
        assert_eq!(read.html, read.text);
    }

    #[test]
    fn a_blank_html_alternative_is_passed_over_and_none_left_is_no_html() {
        let blank = leaf("text/html", b" \n\t", "utf-8");
        let read = read_bodies(
            &container(
                "multipart/alternative",
                json!([
                    blank.clone(),
                    leaf("text/plain", b"plain", "utf-8"),
                    leaf("text/html", b"<p></p>", "utf-8"),
                ]),
            ),
            ALL,
        );
        // Markup that renders to nothing is not blank: it is the HTML sent.
        assert_eq!(texts(&read, &read.html, false), ["<p></p>"]);
        let read = read_bodies(
            &container(
                "multipart/alternative",
                json!([leaf("text/plain", b"plain", "utf-8"), blank]),
            ),
            ALL,
        );
        assert!(read.html.is_empty());
        assert_eq!(texts(&read, &read.text, false), ["plain"]);
    }

    #[test]
    fn an_alternative_reaches_html_nested_in_a_related_part() {
        let related = container(
            "multipart/related",
            json!([
                leaf("text/html", b"<p>one</p>", "utf-8"),
                { "mimeType": "image/png", "filename": "a.png", "body": { "attachmentId": "A" } },
                leaf("text/html", b"<p>two</p>", "utf-8"),
            ]),
        );
        let read = read_bodies(
            &container(
                "multipart/alternative",
                json!([leaf("text/plain", b"plain", "utf-8"), related]),
            ),
            ALL,
        );
        assert_eq!(texts(&read, &read.text, false), ["plain"]);
        assert_eq!(
            texts(&read, &read.html, false),
            ["<p>one</p>", "<p>two</p>"]
        );
        assert_eq!(texts(&read, &read.html, true), ["one", "two"]);
    }

    #[test]
    fn alternatives_inside_and_beside_alternatives_each_choose_alone() {
        let inner = container(
            "multipart/alternative",
            json!([
                leaf("text/plain", b"  ", "utf-8"),
                leaf("text/html", b"<p>x</p>", "utf-8"),
            ]),
        );
        let read = read_bodies(
            &container(
                "multipart/alternative",
                json!([inner, leaf("text/plain", b"y", "utf-8")]),
            ),
            ALL,
        );
        assert_eq!(texts(&read, &read.text, false), ["y"]);
        assert_eq!(texts(&read, &read.html, false), ["<p>x</p>"]);
        let read = read_bodies(
            &container(
                "multipart/mixed",
                json!([
                    leaf("text/plain", b"first", "utf-8"),
                    container(
                        "multipart/alternative",
                        json!([
                            leaf("text/plain", b"a", "utf-8"),
                            leaf("text/html", b"<b>A</b>", "utf-8"),
                        ]),
                    ),
                    container(
                        "Multipart/Alternative",
                        json!([
                            leaf("text/html", b"<i>B</i>", "utf-8"),
                            leaf("text/plain", b"b", "utf-8"),
                        ]),
                    ),
                    leaf("text/html", b"<p>last</p>", "utf-8"),
                ]),
            ),
            ALL,
        );
        assert_eq!(texts(&read, &read.text, true), ["first", "a", "b", "last"]);
        assert_eq!(
            texts(&read, &read.html, false),
            ["<b>A</b>", "<i>B</i>", "<p>last</p>"]
        );
    }

    #[test]
    fn several_html_leaves_are_read_in_order() {
        let read = read_bodies(
            &container(
                "multipart/mixed",
                json!([
                    leaf("text/html", b"<p>a</p>", "utf-8"),
                    leaf("text/plain", b"between", "utf-8"),
                    container(
                        "message/rfc822",
                        json!([leaf("text/html", b"<p>b</p>", "utf-8")])
                    ),
                ]),
            ),
            ALL,
        );
        assert_eq!(texts(&read, &read.text, true), ["a", "between", "b"]);
        assert_eq!(texts(&read, &read.html, false), ["<p>a</p>", "<p>b</p>"]);
    }

    #[test]
    fn text_parts_by_reference_are_counted_and_in_no_view() {
        let by_reference = |mime_type: &str| json!({ "mimeType": mime_type, "body": { "attachmentId": "A", "size": 9 } });
        let read = read_bodies(
            &container(
                "multipart/mixed",
                json!([
                    by_reference("text/plain"),
                    by_reference("text/html"),
                    { "mimeType": "text/plain", "filename": "notes.txt",
                      "body": { "attachmentId": "B" } },
                    leaf("text/plain", b"inline", "utf-8"),
                ]),
            ),
            ALL,
        );
        assert_eq!(read.by_reference, 2);
        assert_eq!(texts(&read, &read.text, false), ["inline"]);
        assert!(read.html.is_empty());
    }

    #[test]
    fn windows_1252_high_bytes_decode_to_at_most_three_bytes_each() {
        let high: Vec<u8> = (0x80..=0x9f).collect();
        let read = read_bodies(&leaf("text/html", &high, "windows-1252"), ALL);
        let html = read.leaves[0].decoded.as_deref().unwrap();
        let widths: Vec<usize> = html.chars().map(char::len_utf8).collect();
        assert_eq!(widths.len(), 32);
        // The five undefined bytes are U+FFFD; of the 27 defined, 17 are
        // above U+07FF and 10 (Latin letters and two modifiers) below.
        assert_eq!(widths.iter().filter(|w| **w == 3).count(), 22, "{html}");
        assert_eq!(widths.iter().filter(|w| **w == 2).count(), 10, "{html}");
        assert_eq!(read.notes, []);
    }

    #[test]
    fn invalid_utf8_is_replaced_and_noted_once_per_leaf_read() {
        let read = read_bodies(
            &container(
                "multipart/alternative",
                json!([
                    leaf("text/plain", b"a\xffb", "utf-8"),
                    leaf("text/html", b"<p>\xfe</p>", "utf-8"),
                ]),
            ),
            ALL,
        );
        assert_eq!(texts(&read, &read.text, false), ["a\u{fffd}b"]);
        assert_eq!(texts(&read, &read.html, false), ["<p>\u{fffd}</p>"]);
        assert_eq!(read.notes, [Note::InvalidUtf8, Note::InvalidUtf8]);
        // The HTML alternative's note is only for a form that reads it.
        let only_text = Wanted {
            text: true,
            ..Wanted::default()
        };
        let read = read_bodies(
            &container(
                "multipart/alternative",
                json!([
                    leaf("text/plain", b"ok", "utf-8"),
                    leaf("text/html", b"<p>\xfe</p>", "KOI8-R"),
                    { "mimeType": "multipart/related" },
                ]),
            ),
            only_text,
        );
        assert_eq!(
            read.notes,
            [Note::UnlistedChildren {
                mime_type: "multipart/related".into(),
                depth: 1,
            }]
        );
    }

    #[test]
    fn what_no_form_asked_for_reads_is_not_kept_or_rendered() {
        let message = container(
            "multipart/alternative",
            json!([
                leaf("text/plain", b"plain", "utf-8"),
                leaf("text/html", b"<p>rich</p>", "utf-8"),
            ]),
        );
        let html = read_bodies(
            &message,
            Wanted {
                html: true,
                ..Wanted::default()
            },
        );
        assert!(html.text.is_empty());
        assert_eq!(html.html, [1]);
        assert_eq!(
            html.leaves,
            [
                Leaf {
                    source: Source::Plain,
                    decoded: None,
                    rendering: None,
                },
                Leaf {
                    source: Source::Html,
                    decoded: Some("<p>rich</p>".to_string()),
                    rendering: None,
                },
            ]
        );
        let rendered = read_bodies(
            &message,
            Wanted {
                rendered: true,
                ..Wanted::default()
            },
        );
        assert_eq!(rendered.leaves[1].decoded, None);
        assert_eq!(rendered.leaves[1].rendering.as_deref(), Some("rich"));
        let text = read_bodies(
            &message,
            Wanted {
                text: true,
                ..Wanted::default()
            },
        );
        assert_eq!(texts(&text, &text.text, false), ["plain"]);
        assert_eq!(
            text.leaves[1],
            Leaf {
                source: Source::Html,
                decoded: None,
                rendering: None,
            }
        );
    }

    #[test]
    fn the_text_view_is_what_extract_yields() {
        let message = container(
            "multipart/mixed",
            json!([
                container(
                    "multipart/alternative",
                    json!([
                        leaf("text/plain", b" ", "utf-8"),
                        leaf("text/html", b"<p>caf\xe9</p>", "iso-8859-1"),
                    ]),
                ),
                leaf("text/plain", b"tail", "KOI8-R"),
            ]),
        );
        let extracted = extracted(&message).unwrap();
        let read = read_bodies(&message, ALL);
        let text: Vec<(String, Source)> = read
            .text
            .iter()
            .map(|&i| {
                let leaf = &read.leaves[i];
                (leaf.text().unwrap().to_string(), leaf.source)
            })
            .collect();
        assert_eq!(text, extracted.text_parts);
        assert_eq!(read.notes, extracted.notes);
    }

    #[test]
    fn a_leaf_whose_data_is_not_base64url_names_its_part() {
        let part = json!({ "partId": "1.1", "mimeType": "text/html", "body": { "data": "*" } });
        assert_eq!(
            bodies(&Part::from_value(&part).unwrap(), ALL).map(|(bodies, _)| bodies),
            Err(WalkError::Base64("1.1".to_string()))
        );
    }
}
