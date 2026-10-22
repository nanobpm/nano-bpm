//! A compact, dependency-free regular-expression engine.
//!
//! Supports the common subset used by the FEEL `matches`, `replace` and `split`
//! builtins: literals, `.`, character classes (`[...]` with ranges, negation and
//! the `\d \w \s` shorthands), anchors `^`/`$`, the quantifiers `* + ?` and
//! `{n}`/`{n,}`/`{n,m}`, alternation `|`, and capturing/non-capturing groups.
//! Flags `i` (case-insensitive), `s` (dot matches newline) and `m` (multiline
//! anchors) are honoured. Matching is a backtracking VM with capture support.

#[derive(Clone, Debug)]
enum Inst {
    Char(char),
    Any,
    Class(Vec<ClassItem>, bool), // items, negated
    Start,
    End,
    Save(usize),
    Split(usize, usize),
    Jmp(usize),
    Match,
}

#[derive(Clone, Debug)]
enum ClassItem {
    Single(char),
    Range(char, char),
    Digit,
    NotDigit,
    Word,
    NotWord,
    Space,
    NotSpace,
}

pub struct Regex {
    prog: Vec<Inst>,
    groups: usize,
    icase: bool,
    dotall: bool,
    multiline: bool,
}

struct Parser {
    chars: Vec<char>,
    pos: usize,
    group_count: usize,
}

impl Regex {
    pub fn new(pattern: &str, flags: &str) -> Option<Regex> {
        let mut p = Parser {
            chars: pattern.chars().collect(),
            pos: 0,
            group_count: 0,
        };
        let ast = p.parse_alt()?;
        if p.pos != p.chars.len() {
            return None;
        }
        let mut prog = Vec::new();
        prog.push(Inst::Save(0));
        emit(&ast, &mut prog);
        prog.push(Inst::Save(1));
        prog.push(Inst::Match);
        Some(Regex {
            prog,
            groups: p.group_count,
            icase: flags.contains('i'),
            dotall: flags.contains('s'),
            multiline: flags.contains('m'),
        })
    }

    pub fn is_match(&self, input: &str) -> bool {
        self.find(&input.chars().collect::<Vec<_>>(), 0).is_some()
    }

    /// Returns the captures of the first match: `caps[0]` is the whole match,
    /// `caps[n]` is group n. Each is `(start, end)` in `char` units.
    fn find(&self, input: &[char], from: usize) -> Option<Vec<Option<(usize, usize)>>> {
        for start in from..=input.len() {
            if let Some(caps) = self.find_at(input, start) {
                return Some(caps);
            }
        }
        None
    }

    fn find_at(&self, input: &[char], start: usize) -> Option<Vec<Option<(usize, usize)>>> {
        let mut caps = vec![None; (self.groups + 1) * 2];
        if self.run(0, input, start, &mut caps) {
            let mut out = vec![None; self.groups + 1];
            for g in 0..=self.groups {
                if let (Some(s), Some(e)) = (caps[g * 2], caps[g * 2 + 1]) {
                    out[g] = Some((s, e));
                }
            }
            Some(out)
        } else {
            None
        }
    }

    fn run(
        &self,
        mut pc: usize,
        input: &[char],
        mut sp: usize,
        caps: &mut Vec<Option<usize>>,
    ) -> bool {
        loop {
            match &self.prog[pc] {
                Inst::Char(c) => {
                    if sp < input.len() && self.char_eq(input[sp], *c) {
                        pc += 1;
                        sp += 1;
                    } else {
                        return false;
                    }
                }
                Inst::Any => {
                    if sp < input.len() && (self.dotall || input[sp] != '\n') {
                        pc += 1;
                        sp += 1;
                    } else {
                        return false;
                    }
                }
                Inst::Class(items, negated) => {
                    if sp < input.len() && self.class_match(input[sp], items, *negated) {
                        pc += 1;
                        sp += 1;
                    } else {
                        return false;
                    }
                }
                Inst::Start => {
                    let ok = sp == 0 || (self.multiline && input[sp - 1] == '\n');
                    if ok {
                        pc += 1;
                    } else {
                        return false;
                    }
                }
                Inst::End => {
                    let ok = sp == input.len() || (self.multiline && input[sp] == '\n');
                    if ok {
                        pc += 1;
                    } else {
                        return false;
                    }
                }
                Inst::Save(slot) => {
                    let old = caps[*slot];
                    caps[*slot] = Some(sp);
                    if self.run(pc + 1, input, sp, caps) {
                        return true;
                    }
                    caps[*slot] = old;
                    return false;
                }
                Inst::Split(a, b) => {
                    if self.run(*a, input, sp, caps) {
                        return true;
                    }
                    pc = *b;
                }
                Inst::Jmp(t) => pc = *t,
                Inst::Match => return true,
            }
        }
    }

    fn char_eq(&self, a: char, b: char) -> bool {
        if self.icase {
            a.eq_ignore_ascii_case(&b) || a.to_lowercase().eq(b.to_lowercase())
        } else {
            a == b
        }
    }

    fn class_match(&self, c: char, items: &[ClassItem], negated: bool) -> bool {
        let mut hit = false;
        for item in items {
            let m = match item {
                ClassItem::Single(x) => self.char_eq(c, *x),
                ClassItem::Range(lo, hi) => {
                    (*lo..=*hi).contains(&c)
                        || (self.icase
                            && c.to_ascii_lowercase() >= lo.to_ascii_lowercase()
                            && c.to_ascii_lowercase() <= hi.to_ascii_lowercase())
                }
                ClassItem::Digit => c.is_ascii_digit(),
                ClassItem::NotDigit => !c.is_ascii_digit(),
                ClassItem::Word => c.is_alphanumeric() || c == '_',
                ClassItem::NotWord => !(c.is_alphanumeric() || c == '_'),
                ClassItem::Space => c.is_whitespace(),
                ClassItem::NotSpace => !c.is_whitespace(),
            };
            if m {
                hit = true;
                break;
            }
        }
        hit ^ negated
    }

    /// Replaces every match with `replacement`, expanding `$0`/`$1`… group refs.
    pub fn replace_all(&self, input: &str, replacement: &str) -> String {
        let chars: Vec<char> = input.chars().collect();
        let mut out = String::new();
        let mut pos = 0;
        while pos <= chars.len() {
            match self.find(&chars, pos) {
                Some(caps) => {
                    // Capture group 0 (the whole match) is always present
                    // when `find` returns `Some`.
                    #[allow(
                        clippy::unwrap_used,
                        reason = "group 0 is the whole match, always Some on a hit"
                    )]
                    let (s, e) = caps[0].unwrap();
                    out.extend(&chars[pos..s]);
                    expand(replacement, &caps, &chars, &mut out);
                    if e > pos {
                        pos = e;
                    } else {
                        // Zero-width match: emit one char and advance.
                        if s < chars.len() {
                            out.push(chars[s]);
                        }
                        pos = s + 1;
                    }
                }
                None => {
                    out.extend(&chars[pos..]);
                    break;
                }
            }
        }
        out
    }

    /// Splits `input` on matches, returning the pieces between matches.
    pub fn split(&self, input: &str) -> Vec<String> {
        let chars: Vec<char> = input.chars().collect();
        let mut out = Vec::new();
        let mut last = 0;
        let mut pos = 0;
        while pos <= chars.len() {
            match self.find(&chars, pos) {
                Some(caps) => {
                    // Capture group 0 (the whole match) is always present
                    // when `find` returns `Some`.
                    #[allow(
                        clippy::unwrap_used,
                        reason = "group 0 is the whole match, always Some on a hit"
                    )]
                    let (s, e) = caps[0].unwrap();
                    if e == s {
                        pos = s + 1;
                        continue;
                    }
                    out.push(chars[last..s].iter().collect());
                    last = e;
                    pos = e;
                }
                None => break,
            }
        }
        out.push(chars[last..].iter().collect());
        out
    }

    /// Returns every non-overlapping whole match in `input`, in order (the
    /// `extract` builtin's `findAllIn` semantics).
    pub fn find_all(&self, input: &str) -> Vec<String> {
        let chars: Vec<char> = input.chars().collect();
        let mut out = Vec::new();
        let mut pos = 0;
        while pos <= chars.len() {
            match self.find(&chars, pos) {
                Some(caps) => {
                    // Capture group 0 (the whole match) is always present
                    // when `find` returns `Some`.
                    #[allow(
                        clippy::unwrap_used,
                        reason = "group 0 is the whole match, always Some on a hit"
                    )]
                    let (s, e) = caps[0].unwrap();
                    out.push(chars[s..e].iter().collect());
                    // Advance past the match; step one char on a zero-width hit.
                    pos = if e > s { e } else { s + 1 };
                }
                None => break,
            }
        }
        out
    }
}

fn expand(template: &str, caps: &[Option<(usize, usize)>], input: &[char], out: &mut String) {
    let tchars: Vec<char> = template.chars().collect();
    let mut i = 0;
    while i < tchars.len() {
        if tchars[i] == '$' && i + 1 < tchars.len() && tchars[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut num = String::new();
            while j < tchars.len() && tchars[j].is_ascii_digit() {
                num.push(tchars[j]);
                j += 1;
            }
            let g: usize = num.parse().unwrap_or(usize::MAX);
            if g < caps.len() {
                if let Some((s, e)) = caps[g] {
                    out.extend(&input[s..e]);
                }
            }
            i = j;
        } else {
            out.push(tchars[i]);
            i += 1;
        }
    }
}

// --- Regex AST + compiler ---------------------------------------------------

enum Ast {
    Empty,
    Char(char),
    Any,
    Class(Vec<ClassItem>, bool),
    Start,
    End,
    Group(Option<usize>, Box<Ast>),
    Concat(Vec<Ast>),
    Alt(Vec<Ast>),
    Star(Box<Ast>),
    Plus(Box<Ast>),
    Quest(Box<Ast>),
    Repeat(Box<Ast>, usize, Option<usize>),
}

fn emit(ast: &Ast, prog: &mut Vec<Inst>) {
    match ast {
        Ast::Empty => {}
        Ast::Char(c) => prog.push(Inst::Char(*c)),
        Ast::Any => prog.push(Inst::Any),
        Ast::Class(items, neg) => prog.push(Inst::Class(items.clone(), *neg)),
        Ast::Start => prog.push(Inst::Start),
        Ast::End => prog.push(Inst::End),
        Ast::Group(num, inner) => {
            if let Some(n) = num {
                prog.push(Inst::Save(n * 2));
            }
            emit(inner, prog);
            if let Some(n) = num {
                prog.push(Inst::Save(n * 2 + 1));
            }
        }
        Ast::Concat(items) => {
            for it in items {
                emit(it, prog);
            }
        }
        Ast::Alt(branches) => emit_alt(branches, prog),
        Ast::Star(inner) => {
            let l1 = prog.len();
            prog.push(Inst::Split(0, 0));
            let body = prog.len();
            emit(inner, prog);
            prog.push(Inst::Jmp(l1));
            let l3 = prog.len();
            prog[l1] = Inst::Split(body, l3);
        }
        Ast::Plus(inner) => {
            let body = prog.len();
            emit(inner, prog);
            let split = prog.len();
            prog.push(Inst::Split(body, split + 1));
        }
        Ast::Quest(inner) => {
            let l1 = prog.len();
            prog.push(Inst::Split(0, 0));
            let body = prog.len();
            emit(inner, prog);
            let out = prog.len();
            prog[l1] = Inst::Split(body, out);
        }
        Ast::Repeat(inner, min, max) => {
            for _ in 0..*min {
                emit(inner, prog);
            }
            match max {
                Some(max) => {
                    for _ in *min..*max {
                        let l1 = prog.len();
                        prog.push(Inst::Split(0, 0));
                        let body = prog.len();
                        emit(inner, prog);
                        let out = prog.len();
                        prog[l1] = Inst::Split(body, out);
                    }
                }
                None => emit(&Ast::Star(Box::new(clone_ast(inner))), prog),
            }
        }
    }
}

fn emit_alt(branches: &[Ast], prog: &mut Vec<Inst>) {
    if branches.len() == 1 {
        emit(&branches[0], prog);
        return;
    }
    let split = prog.len();
    prog.push(Inst::Split(0, 0));
    let first = prog.len();
    emit(&branches[0], prog);
    let jmp = prog.len();
    prog.push(Inst::Jmp(0));
    let second = prog.len();
    emit_alt(&branches[1..], prog);
    let end = prog.len();
    prog[split] = Inst::Split(first, second);
    prog[jmp] = Inst::Jmp(end);
}

fn clone_ast(a: &Ast) -> Ast {
    match a {
        Ast::Empty => Ast::Empty,
        Ast::Char(c) => Ast::Char(*c),
        Ast::Any => Ast::Any,
        Ast::Class(i, n) => Ast::Class(i.clone(), *n),
        Ast::Start => Ast::Start,
        Ast::End => Ast::End,
        Ast::Group(n, i) => Ast::Group(*n, Box::new(clone_ast(i))),
        Ast::Concat(v) => Ast::Concat(v.iter().map(clone_ast).collect()),
        Ast::Alt(v) => Ast::Alt(v.iter().map(clone_ast).collect()),
        Ast::Star(i) => Ast::Star(Box::new(clone_ast(i))),
        Ast::Plus(i) => Ast::Plus(Box::new(clone_ast(i))),
        Ast::Quest(i) => Ast::Quest(Box::new(clone_ast(i))),
        Ast::Repeat(i, a2, b) => Ast::Repeat(Box::new(clone_ast(i)), *a2, *b),
    }
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn parse_alt(&mut self) -> Option<Ast> {
        let mut branches = vec![self.parse_concat()?];
        while self.peek() == Some('|') {
            self.pos += 1;
            branches.push(self.parse_concat()?);
        }
        if branches.len() == 1 {
            branches.pop()
        } else {
            Some(Ast::Alt(branches))
        }
    }

    fn parse_concat(&mut self) -> Option<Ast> {
        let mut items = Vec::new();
        while let Some(c) = self.peek() {
            if c == '|' || c == ')' {
                break;
            }
            items.push(self.parse_quant()?);
        }
        if items.is_empty() {
            Some(Ast::Empty)
        } else if items.len() == 1 {
            items.pop()
        } else {
            Some(Ast::Concat(items))
        }
    }

    fn parse_quant(&mut self) -> Option<Ast> {
        let atom = self.parse_atom()?;
        let q = match self.peek() {
            Some('*') => {
                self.pos += 1;
                Ast::Star(Box::new(atom))
            }
            Some('+') => {
                self.pos += 1;
                Ast::Plus(Box::new(atom))
            }
            Some('?') => {
                self.pos += 1;
                Ast::Quest(Box::new(atom))
            }
            Some('{') => {
                if let Some((min, max)) = self.parse_repeat() {
                    Ast::Repeat(Box::new(atom), min, max)
                } else {
                    return Some(atom);
                }
            }
            _ => return Some(atom),
        };
        // Accept and ignore a lazy `?` marker.
        if self.peek() == Some('?') {
            self.pos += 1;
        }
        Some(q)
    }

    fn parse_repeat(&mut self) -> Option<(usize, Option<usize>)> {
        let save = self.pos;
        self.pos += 1; // {
        let mut min = String::new();
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                min.push(c);
                self.pos += 1;
            } else {
                break;
            }
        }
        let (lo, hi) = if self.peek() == Some(',') {
            self.pos += 1;
            let mut max = String::new();
            while let Some(c) = self.peek() {
                if c.is_ascii_digit() {
                    max.push(c);
                    self.pos += 1;
                } else {
                    break;
                }
            }
            let lo = min.parse().unwrap_or(0);
            let hi = if max.is_empty() {
                None
            } else {
                Some(max.parse().ok()?)
            };
            (lo, hi)
        } else {
            let n = min.parse().ok()?;
            (n, Some(n))
        };
        if self.peek() != Some('}') {
            self.pos = save;
            return None;
        }
        self.pos += 1;
        Some((lo, hi))
    }

    fn parse_atom(&mut self) -> Option<Ast> {
        match self.peek()? {
            '(' => {
                self.pos += 1;
                let num = if self.chars.get(self.pos) == Some(&'?')
                    && self.chars.get(self.pos + 1) == Some(&':')
                {
                    self.pos += 2;
                    None
                } else {
                    self.group_count += 1;
                    Some(self.group_count)
                };
                let inner = self.parse_alt()?;
                if self.peek() != Some(')') {
                    return None;
                }
                self.pos += 1;
                Some(Ast::Group(num, Box::new(inner)))
            }
            '[' => self.parse_class(),
            '.' => {
                self.pos += 1;
                Some(Ast::Any)
            }
            '^' => {
                self.pos += 1;
                Some(Ast::Start)
            }
            '$' => {
                self.pos += 1;
                Some(Ast::End)
            }
            '\\' => {
                self.pos += 1;
                let c = self.peek()?;
                self.pos += 1;
                Some(escape_atom(c))
            }
            c => {
                self.pos += 1;
                Some(Ast::Char(c))
            }
        }
    }

    fn parse_class(&mut self) -> Option<Ast> {
        self.pos += 1; // [
        let negated = self.peek() == Some('^');
        if negated {
            self.pos += 1;
        }
        let mut items = Vec::new();
        while let Some(c) = self.peek() {
            if c == ']' {
                self.pos += 1;
                return Some(Ast::Class(items, negated));
            }
            if c == '\\' {
                self.pos += 1;
                let e = self.peek()?;
                self.pos += 1;
                items.push(match e {
                    'd' => ClassItem::Digit,
                    'D' => ClassItem::NotDigit,
                    'w' => ClassItem::Word,
                    'W' => ClassItem::NotWord,
                    's' => ClassItem::Space,
                    'S' => ClassItem::NotSpace,
                    'n' => ClassItem::Single('\n'),
                    't' => ClassItem::Single('\t'),
                    'r' => ClassItem::Single('\r'),
                    other => ClassItem::Single(other),
                });
                continue;
            }
            // Range a-z?
            if self.chars.get(self.pos + 1) == Some(&'-')
                && self.chars.get(self.pos + 2).is_some_and(|&x| x != ']')
            {
                let lo = c;
                let hi = self.chars[self.pos + 2];
                self.pos += 3;
                items.push(ClassItem::Range(lo, hi));
            } else {
                self.pos += 1;
                items.push(ClassItem::Single(c));
            }
        }
        None
    }
}

fn escape_atom(c: char) -> Ast {
    match c {
        'd' => Ast::Class(vec![ClassItem::Digit], false),
        'D' => Ast::Class(vec![ClassItem::Digit], true),
        'w' => Ast::Class(vec![ClassItem::Word], false),
        'W' => Ast::Class(vec![ClassItem::Word], true),
        's' => Ast::Class(vec![ClassItem::Space], false),
        'S' => Ast::Class(vec![ClassItem::Space], true),
        'n' => Ast::Char('\n'),
        't' => Ast::Char('\t'),
        'r' => Ast::Char('\r'),
        other => Ast::Char(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_matches() {
        assert!(Regex::new("^a.c$", "").unwrap().is_match("abc"));
        assert!(!Regex::new("^a.c$", "").unwrap().is_match("abbc"));
        assert!(Regex::new("[0-9]+", "").unwrap().is_match("xx42"));
        assert!(Regex::new("foo|bar", "").unwrap().is_match("a bar b"));
        assert!(Regex::new("AB", "i").unwrap().is_match("xxabyy"));
        assert!(Regex::new("a{2,3}", "").unwrap().is_match("aaa"));
        assert!(!Regex::new("^a{2,3}$", "").unwrap().is_match("a"));
    }

    #[test]
    fn replace_and_split() {
        let re = Regex::new("a", "").unwrap();
        assert_eq!(re.replace_all("banana", "o"), "bonono");
        let re = Regex::new("(\\w+)@(\\w+)", "").unwrap();
        assert_eq!(re.replace_all("a@b", "$2.$1"), "b.a");
        let re = Regex::new("\\s*,\\s*", "").unwrap();
        assert_eq!(re.split("a, b ,c"), vec!["a", "b", "c"]);
    }
}
