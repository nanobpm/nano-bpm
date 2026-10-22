//! The FEEL tokenizer.

use super::error::FeelError;

#[derive(Clone, Debug, PartialEq)]
pub enum Tok {
    Num(f64, bool), // value, is_integer
    Str(String),
    At(String), // @"…" temporal literal
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    Power,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Dot,
    DotDot,
    Comma,
    Colon,
}

pub fn tokenize(src: &str) -> Result<Vec<Tok>, FeelError> {
    let chars: Vec<char> = src.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '+' => {
                tokens.push(Tok::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Tok::Minus);
                i += 1;
            }
            '*' => {
                if chars.get(i + 1) == Some(&'*') {
                    tokens.push(Tok::Power);
                    i += 2;
                } else {
                    tokens.push(Tok::Star);
                    i += 1;
                }
            }
            '/' => {
                tokens.push(Tok::Slash);
                i += 1;
            }
            '(' => {
                tokens.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Tok::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Tok::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Tok::RBracket);
                i += 1;
            }
            '{' => {
                tokens.push(Tok::LBrace);
                i += 1;
            }
            '}' => {
                tokens.push(Tok::RBrace);
                i += 1;
            }
            ',' => {
                tokens.push(Tok::Comma);
                i += 1;
            }
            ':' => {
                tokens.push(Tok::Colon);
                i += 1;
            }
            '.' => {
                if chars.get(i + 1) == Some(&'.') {
                    tokens.push(Tok::DotDot);
                    i += 2;
                } else {
                    tokens.push(Tok::Dot);
                    i += 1;
                }
            }
            '=' => {
                // Accept both `=` and `==` as equality.
                if chars.get(i + 1) == Some(&'=') {
                    i += 2;
                } else {
                    i += 1;
                }
                tokens.push(Tok::Eq);
            }
            '!' => {
                if chars.get(i + 1) == Some(&'=') {
                    tokens.push(Tok::Ne);
                    i += 2;
                } else {
                    return Err(FeelError("unexpected '!'".to_string()));
                }
            }
            '<' => {
                if chars.get(i + 1) == Some(&'=') {
                    tokens.push(Tok::Le);
                    i += 2;
                } else {
                    tokens.push(Tok::Lt);
                    i += 1;
                }
            }
            '>' => {
                if chars.get(i + 1) == Some(&'=') {
                    tokens.push(Tok::Ge);
                    i += 2;
                } else {
                    tokens.push(Tok::Gt);
                    i += 1;
                }
            }
            '@' => {
                let q = chars.get(i + 1).copied();
                match q {
                    Some(qc @ ('"' | '\'')) => {
                        let (s, next) = lex_string(&chars, i + 1, qc)?;
                        tokens.push(Tok::At(s));
                        i = next;
                    }
                    _ => {
                        return Err(FeelError("expected a string after '@'".to_string()));
                    }
                }
            }
            '"' | '\'' => {
                let (s, next) = lex_string(&chars, i, c)?;
                tokens.push(Tok::Str(s));
                i = next;
            }
            c if c.is_ascii_digit() => {
                let (tok, next) = lex_number(&chars, i)?;
                tokens.push(tok);
                i = next;
            }
            c if is_ident_start(c) => {
                let start = i;
                i += 1;
                while i < chars.len() && is_ident_part(chars[i]) {
                    i += 1;
                }
                tokens.push(Tok::Ident(chars[start..i].iter().collect()));
            }
            other => return Err(FeelError(format!("unexpected character '{other}'"))),
        }
    }
    Ok(tokens)
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_part(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn lex_string(chars: &[char], start: usize, quote: char) -> Result<(String, usize), FeelError> {
    let mut s = String::new();
    let mut i = start + 1;
    while i < chars.len() {
        let c = chars[i];
        if c == quote {
            return Ok((s, i + 1));
        }
        if c == '\\' {
            i += 1;
            match chars.get(i) {
                Some('"') => s.push('"'),
                Some('\'') => s.push('\''),
                Some('\\') => s.push('\\'),
                Some('n') => s.push('\n'),
                Some('r') => s.push('\r'),
                Some('t') => s.push('\t'),
                Some(other) => s.push(*other),
                None => return Err(FeelError("unterminated string escape".to_string())),
            }
            i += 1;
        } else {
            s.push(c);
            i += 1;
        }
    }
    Err(FeelError("unterminated string".to_string()))
}

fn lex_number(chars: &[char], start: usize) -> Result<(Tok, usize), FeelError> {
    let mut i = start;
    while i < chars.len() && chars[i].is_ascii_digit() {
        i += 1;
    }
    let mut is_int = true;
    if i < chars.len() && chars[i] == '.' && chars.get(i + 1).is_some_and(|c| c.is_ascii_digit()) {
        is_int = false;
        i += 1;
        while i < chars.len() && chars[i].is_ascii_digit() {
            i += 1;
        }
    }
    let text: String = chars[start..i].iter().collect();
    let value: f64 = text
        .parse()
        .map_err(|_| FeelError(format!("invalid number '{text}'")))?;
    Ok((Tok::Num(value, is_int), i))
}
