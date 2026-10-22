//! A tiny, dependency-free XML scanner shared by the [`crate::bpmn`] and
//! [`crate::dmn`] parsers, and by the console's Urban-specific model scans (e.g.
//! the data-envelope worker-IO derivation).
//!
//! It is deliberately minimal: a hand-rolled, namespace-prefix agnostic scanner
//! that turns a document into a flat stream of [`Token`]s (start/end tags with
//! attributes, and text runs). It handles single- or double-quoted attributes,
//! self-closing tags, comments, processing instructions, `DOCTYPE` and `CDATA`.
//! It does **not** validate the document or build a tree — callers walk the token
//! stream and recognise exactly the elements they understand.

/// An error encountered while scanning XML (malformed markup).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XmlError(pub String);

impl std::fmt::Display for XmlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "malformed XML: {}", self.0)
    }
}

/// A scanned XML token.
pub enum Token {
    Start {
        name: String,
        attrs: Vec<(String, String)>,
        self_closing: bool,
    },
    End {
        name: String,
    },
    Text(String),
}

/// Returns the local name of a (possibly namespace-prefixed) name: `bpmn:process`
/// -> `process`, `text` -> `text`.
pub fn local_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

/// Finds an attribute by its local name (ignoring any namespace prefix).
pub fn attr<'a>(attrs: &'a [(String, String)], local: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| local_name(k) == local)
        .map(|(_, v)| v.as_str())
}

/// A tiny, allocation-light XML scanner. Handles elements, attributes (single or
/// double quoted), self-closing tags, comments, processing instructions,
/// `DOCTYPE`, and `CDATA`. It does not validate the document.
pub fn tokenize(xml: &str) -> Result<Vec<Token>, XmlError> {
    let bytes = xml.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] != b'<' {
            // Text run up to the next '<'.
            let start = i;
            while i < bytes.len() && bytes[i] != b'<' {
                i += 1;
            }
            let text = &xml[start..i];
            if !text.trim().is_empty() {
                tokens.push(Token::Text(unescape(text)));
            }
            continue;
        }

        // We are at '<'.
        if xml[i..].starts_with("<!--") {
            let end = find(xml, i + 4, "-->")?;
            i = end + 3;
        } else if xml[i..].starts_with("<![CDATA[") {
            let end = find(xml, i + 9, "]]>")?;
            tokens.push(Token::Text(xml[i + 9..end].to_string()));
            i = end + 3;
        } else if xml[i..].starts_with("<!") || xml[i..].starts_with("<?") {
            // DOCTYPE / processing instruction / XML declaration: skip to '>'.
            let end = find(xml, i + 2, ">")?;
            i = end + 1;
        } else if xml[i..].starts_with("</") {
            let end = find(xml, i + 2, ">")?;
            let name = xml[i + 2..end].trim().to_string();
            tokens.push(Token::End { name });
            i = end + 1;
        } else {
            // Start (or self-closing) tag. Find the closing '>' that is not
            // inside a quoted attribute value.
            let end = find_tag_end(xml, i + 1)?;
            let inner = xml[i + 1..end].trim();
            let (inner, self_closing) = match inner.strip_suffix('/') {
                Some(stripped) => (stripped.trim_end(), true),
                None => (inner, false),
            };
            let (name, attrs) = parse_tag(inner)?;
            tokens.push(Token::Start {
                name,
                attrs,
                self_closing,
            });
            i = end + 1;
        }
    }

    Ok(tokens)
}

/// Finds the byte index of the next `needle` at or after `from`.
fn find(haystack: &str, from: usize, needle: &str) -> Result<usize, XmlError> {
    haystack[from..]
        .find(needle)
        .map(|p| from + p)
        .ok_or_else(|| XmlError(format!("expected `{needle}`")))
}

/// Finds the `>` ending a start tag, skipping any inside quoted attribute values.
fn find_tag_end(xml: &str, from: usize) -> Result<usize, XmlError> {
    let bytes = xml.as_bytes();
    let mut i = from;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == b'"' || c == b'\'' => quote = Some(c),
            None if c == b'>' => return Ok(i),
            None => {}
        }
        i += 1;
    }
    Err(XmlError("unterminated tag".to_string()))
}

/// Splits a tag's inner text into its name and attributes.
fn parse_tag(inner: &str) -> Result<(String, Vec<(String, String)>), XmlError> {
    let inner = inner.trim();
    let name_end = inner
        .find(|c: char| c.is_whitespace())
        .unwrap_or(inner.len());
    let name = inner[..name_end].to_string();
    if name.is_empty() {
        return Err(XmlError("empty tag name".to_string()));
    }

    let mut attrs = Vec::new();
    let rest = inner[name_end..].trim_start();
    let bytes = rest.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Attribute name up to '='.
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let key = rest[key_start..i].trim();
        // Skip whitespace and the '='.
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            // Valueless attribute; ignore.
            if key.is_empty() {
                i += 1;
            }
            continue;
        }
        i += 1; // consume '='
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || (bytes[i] != b'"' && bytes[i] != b'\'') {
            return Err(XmlError(format!("attribute {key} has no quoted value")));
        }
        let q = bytes[i];
        i += 1;
        let val_start = i;
        while i < bytes.len() && bytes[i] != q {
            i += 1;
        }
        if i >= bytes.len() {
            return Err(XmlError("unterminated attribute".to_string()));
        }
        let value = unescape(&rest[val_start..i]);
        i += 1; // consume closing quote
        if !key.is_empty() {
            attrs.push((key.to_string(), value));
        }
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
    }

    Ok((name, attrs))
}

/// Expands the five predefined XML entities and numeric character references.
///
/// Handles the five named entities (`&lt;`, `&gt;`, `&quot;`, `&apos;`, `&amp;`)
/// as well as decimal (`&#NN;`) and hexadecimal (`&#xHH;`) numeric character
/// references. Camunda Modeler emits `&#34;` for the double-quotes inside a FEEL
/// string literal placed in an attribute value, so decoding numeric references
/// here is required for such attributes (e.g. a `zeebe:subscription`
/// `correlationKey`) to reach FEEL correctly.
///
/// A single left-to-right pass is used so that already-decoded text (in
/// particular a `&` produced by expanding `&amp;`) is never re-scanned as the
/// start of another entity. Any unrecognised or malformed `&…` sequence is left
/// verbatim.
pub fn unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'&' {
            // Copy the current UTF-8 character wholesale. `i` is always on a
            // char boundary (we advance by `len_utf8`), so there is a next char.
            let Some(ch) = s[i..].chars().next() else {
                break;
            };
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        // Look for the terminating ';' of an entity reference. We only advance
        // over bytes that can legitimately appear in a reference body — ASCII
        // letters/digits (named entities and numeric digits) and the leading
        // `#` — and stop at the first byte that cannot. This keeps the whole
        // pass O(n): the scan from one `&` stops at the next `&` at the latest
        // (a `&` is not a body byte), so the scanned spans never overlap and a
        // pathological input (many `&` with no nearby `;`) can no longer be
        // O(n²). Unlike a fixed byte cap it also decodes numeric references with
        // arbitrary leading zeros (e.g. `&#x00000022;`). Body bytes are ASCII,
        // so scanning bytes cannot split a multi-byte character.
        let rest = &bytes[i..];
        let mut j = 1; // skip the leading '&'
        while j < rest.len() && is_entity_body_byte(rest[j]) {
            j += 1;
        }
        if j < rest.len() && rest[j] == b';' {
            let entity = &s[i + 1..i + j]; // between '&' and ';'
            if let Some(ch) = decode_entity(entity) {
                out.push(ch);
                i += j + 1; // consume through the ';'
                continue;
            }
        }
        // Not a recognised entity — emit the '&' literally and move on.
        out.push('&');
        i += 1;
    }
    out
}

/// Whether `b` can appear in the body of an entity reference (between `&` and
/// `;`): an ASCII letter (named entities and the `x`/`X` hex marker), an ASCII
/// digit (decimal/hex references), or the leading `#` of a numeric reference.
/// Used to bound the terminating-`;` scan in [`unescape`] to O(n).
fn is_entity_body_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'#'
}

/// Decodes the body of an entity reference (the text between `&` and `;`).
///
/// Returns `Some(char)` for the five named entities and for valid decimal
/// (`#NN`) / hexadecimal (`#xHH`) numeric character references, or `None` for
/// anything unrecognised or malformed (which callers leave verbatim).
///
/// Numeric references that resolve to a code point not permitted by the XML 1.0
/// `Char` production (e.g. `&#0;` → NUL, or other C0 control characters) are
/// rejected as `None` too, so we never smuggle control characters into
/// downstream parsers / FEEL — the original `&#...;` text is left verbatim.
fn decode_entity(entity: &str) -> Option<char> {
    match entity {
        "lt" => return Some('<'),
        "gt" => return Some('>'),
        "quot" => return Some('"'),
        "apos" => return Some('\''),
        "amp" => return Some('&'),
        _ => {}
    }
    let num = entity.strip_prefix('#')?;
    let code = if let Some(hex) = num.strip_prefix('x').or_else(|| num.strip_prefix('X')) {
        u32::from_str_radix(hex, 16).ok()?
    } else {
        num.parse::<u32>().ok()?
    };
    let ch = char::from_u32(code)?;
    is_xml_char(ch).then_some(ch)
}

/// Whether `ch` is permitted by the XML 1.0 `Char` production:
///
/// ```text
/// Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
/// ```
///
/// Surrogates (`#xD800-#xDFFF`) are already excluded by `char`; this additionally
/// rejects the disallowed C0 control characters and the non-characters `#xFFFE` /
/// `#xFFFF`.
fn is_xml_char(ch: char) -> bool {
    matches!(ch,
        '\u{9}' | '\u{A}' | '\u{D}'
        | '\u{20}'..='\u{D7FF}'
        | '\u{E000}'..='\u{FFFD}'
        | '\u{10000}'..='\u{10FFFF}')
}

/// A parsed XML element tree node.
///
/// A convenience layer over [`tokenize`] for callers (like the DMN parser) whose
/// input is small and deeply nested, where walking a tree is far clearer than a
/// flat token stream. `text` holds this element's concatenated **direct** text
/// content (not its descendants').
#[derive(Clone, Debug, Default)]
pub struct Element {
    pub name: String,
    pub attrs: Vec<(String, String)>,
    pub children: Vec<Element>,
    pub text: String,
}

impl Element {
    /// This element's local (namespace-stripped) name.
    pub fn local_name(&self) -> &str {
        local_name(&self.name)
    }

    /// Looks up one of this element's attributes by local name.
    pub fn attr(&self, local: &str) -> Option<&str> {
        attr(&self.attrs, local)
    }

    /// The first direct child with the given local name.
    pub fn child(&self, local: &str) -> Option<&Element> {
        self.children.iter().find(|c| c.local_name() == local)
    }

    /// All direct children with the given local name, in document order.
    pub fn children_named<'a>(&'a self, local: &'a str) -> impl Iterator<Item = &'a Element> + 'a {
        self.children
            .iter()
            .filter(move |c| c.local_name() == local)
    }

    /// The direct text content of the first child with the given local name.
    pub fn child_text(&self, local: &str) -> Option<String> {
        self.child(local).map(|c| c.text.clone())
    }
}

/// Parses `xml` into a single root [`Element`] tree.
///
/// Returns an error if the document has no (or more than one) root element.
pub fn parse_tree(xml: &str) -> Result<Element, XmlError> {
    let tokens = tokenize(xml)?;
    // A sentinel root that collects the single document element as its child.
    let mut stack: Vec<Element> = vec![Element::default()];
    for token in tokens {
        match token {
            Token::Start {
                name,
                attrs,
                self_closing,
            } => {
                let element = Element {
                    name,
                    attrs,
                    children: Vec::new(),
                    text: String::new(),
                };
                if self_closing {
                    // The sentinel root (pushed before the loop) is never popped,
                    // so the stack always has at least one element here.
                    #[allow(
                        clippy::expect_used,
                        reason = "sentinel root keeps the stack non-empty"
                    )]
                    stack
                        .last_mut()
                        .expect("stack never empties")
                        .children
                        .push(element);
                } else {
                    stack.push(element);
                }
            }
            Token::End { .. } => {
                let finished = stack
                    .pop()
                    .ok_or_else(|| XmlError("unbalanced end tag".into()))?;
                if stack.is_empty() {
                    return Err(XmlError("unbalanced end tag".into()));
                }
                // The `is_empty` guard above proves there is a parent to push into.
                #[allow(
                    clippy::unwrap_used,
                    reason = "the is_empty guard above proves the stack is non-empty"
                )]
                stack.last_mut().unwrap().children.push(finished);
            }
            Token::Text(t) => {
                // The sentinel root keeps the stack non-empty for stray text too.
                #[allow(
                    clippy::unwrap_used,
                    reason = "sentinel root keeps the stack non-empty"
                )]
                stack.last_mut().unwrap().text.push_str(&t);
            }
        }
    }
    let mut root = stack
        .pop()
        .ok_or_else(|| XmlError("empty document".into()))?;
    if !stack.is_empty() {
        return Err(XmlError("unbalanced start tag".into()));
    }
    match root.children.len() {
        1 => match root.children.pop() {
            Some(child) => Ok(child),
            None => Err(XmlError("no root element".into())),
        },
        0 => Err(XmlError("no root element".into())),
        _ => Err(XmlError("multiple root elements".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::unescape;

    #[test]
    fn should_decode_the_five_named_entities() {
        assert_eq!(unescape("a &lt; b &gt; c"), "a < b > c");
        assert_eq!(unescape("&quot;q&quot;"), "\"q\"");
        assert_eq!(unescape("it&apos;s"), "it's");
        assert_eq!(unescape("a &amp; b"), "a & b");
    }

    #[test]
    fn should_decode_decimal_numeric_character_references() {
        // Camunda Modeler emits `&#34;` for a double-quote inside an attribute.
        assert_eq!(unescape("=&#34;k&#34;"), "=\"k\"");
        assert_eq!(unescape("&#60;&#62;"), "<>");
    }

    #[test]
    fn should_decode_hexadecimal_numeric_character_references() {
        // Both `&#xHH;` and the (rarer) uppercase `&#XHH;` are accepted.
        assert_eq!(unescape("=&#x22;k&#x22;"), "=\"k\"");
        assert_eq!(unescape("=&#X22;k&#X22;"), "=\"k\"");
    }

    #[test]
    fn should_not_re_decode_an_ampersand_produced_by_amp() {
        // A single left-to-right pass must not treat the `&` yielded by `&amp;`
        // as the start of a further entity: `&amp;#34;` is the literal text
        // `&#34;`, not a decimal reference to `"`.
        assert_eq!(unescape("&amp;#34;"), "&#34;");
        assert_eq!(unescape("&amp;quot;"), "&quot;");
    }

    #[test]
    fn should_leave_unrecognised_or_malformed_sequences_verbatim() {
        assert_eq!(unescape("a & b"), "a & b");
        assert_eq!(unescape("Q&A"), "Q&A");
        assert_eq!(unescape("&unknown;"), "&unknown;");
        assert_eq!(unescape("&#;"), "&#;");
        assert_eq!(unescape("&#xZZ;"), "&#xZZ;");
        // Out-of-range code point (beyond U+10FFFF) is not a valid char.
        assert_eq!(unescape("&#9999999999;"), "&#9999999999;");
    }

    #[test]
    fn should_return_input_unchanged_when_there_is_no_ampersand() {
        assert_eq!(unescape("plain text"), "plain text");
    }

    #[test]
    fn should_leave_non_xml_control_character_references_verbatim() {
        // Code points that resolve to a valid `char` but are NOT permitted by the
        // XML 1.0 `Char` production must be left as their original text rather than
        // smuggling control characters (e.g. NUL) into downstream parsers / FEEL.
        assert_eq!(unescape("&#0;"), "&#0;"); // NUL
        assert_eq!(unescape("&#x0;"), "&#x0;"); // NUL (hex)
        assert_eq!(unescape("&#8;"), "&#8;"); // backspace (C0 control)
        assert_eq!(unescape("&#x1F;"), "&#x1F;"); // unit separator (C0 control)
        assert_eq!(unescape("&#xFFFE;"), "&#xFFFE;"); // non-character
        assert_eq!(unescape("&#xFFFF;"), "&#xFFFF;"); // non-character
                                                      // The XML-permitted control characters (tab, LF, CR) still decode.
        assert_eq!(unescape("&#9;"), "\t");
        assert_eq!(unescape("&#xA;"), "\n");
        assert_eq!(unescape("&#xD;"), "\r");
    }

    #[test]
    fn should_bound_the_scan_for_the_terminating_semicolon() {
        // The scan for a reference's `;` only advances over valid body bytes and
        // stops at the first non-body byte, so it stays O(n) instead of O(n²) on
        // pathological input while still decoding any genuine reference.
        // A long run that is not a known entity leaves the `&` verbatim:
        assert_eq!(unescape("&aaaaaaaaaaa;"), "&aaaaaaaaaaa;");
        // Many ampersands with no nearby `;` are all emitted literally:
        assert_eq!(unescape("&&&&&&&&&&&&&&&"), "&&&&&&&&&&&&&&&");
        // The longest un-padded reference (`&#x10FFFF;`) decodes:
        assert_eq!(unescape("&#x10FFFF;"), "\u{10FFFF}");
        assert_eq!(unescape("&#1114111;"), "\u{10FFFF}");
        // Numeric references with arbitrary leading zeros — far longer than any
        // fixed byte cap — still decode correctly (regression guard):
        assert_eq!(unescape("&#x00000022;"), "\"");
        assert_eq!(unescape("&#000000034;"), "\"");
        assert_eq!(unescape("&#x0000000010FFFF;"), "\u{10FFFF}");
    }
}
