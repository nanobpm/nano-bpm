import Feel.Value
import Feel.Syntax

/-!
# FEEL reference semantics (issue #1230)

A total reference evaluator for the fuzzed FEEL subset, written to mirror the
Rust `engine-core::feel` evaluator (`engine-core/src/feel/eval.rs`) rule for
rule. The correspondences the differential fuzz pins:

* arithmetic `+ - * /` over numbers; `+` also concatenates strings; every other
  operand pairing is a type error, which — as in feel-scala, the FEEL engine
  Zeebe embeds — is a *suppressed failure* yielding `null`, never an
  evaluation error (#1392);
* division by zero yields `null` (not an error);
* `= !=` use FEEL equality (numbers by value, then structural); `null` against
  any value is a boolean, but two non-null values of different types yield
  `null` (#1394); `< <= > >=` are defined only for two numbers or two strings
  and yield `null` otherwise;
* `and`/`or` are three-valued **and short-circuit**, so an error in the
  right operand is masked once the left operand already decides the result
  (`false and (1 + true)` is `false`, not an error);
* `not` maps `null` to `null`; unary `-` and `not` on a wrong type yield `null`;
* `if` with a non-boolean (including `null`) condition takes the `else` branch
  (#1393);
* an unbound variable is `null` (the generator never uses builtin names, for
  which the Rust engine would instead yield a function value).
-/

namespace Feel

/-- An evaluation context: bound variable names to values. An absent name is
`null`. -/
abbrev Ctx := List (String × Val)

/-- Looks a variable up, defaulting an unbound name to `null`. -/
def Ctx.lookup (ctx : Ctx) (name : String) : Val :=
  match ctx.find? (fun p => p.1 == name) with
  | some p => p.2
  | none => .null

/-- Byte-order lexicographic comparison of two strings, matching Rust's
`str::cmp` for the ASCII alphabet the generator uses (a `Char` codepoint order
that coincides with UTF-8 byte order on ASCII). -/
def strCompare (a b : String) : Ordering :=
  let rec go : List Char → List Char → Ordering
    | [], [] => .eq
    | [], _ :: _ => .lt
    | _ :: _, [] => .gt
    | x :: xs, y :: ys =>
      match compare x.val y.val with
      | .eq => go xs ys
      | o => o
  go a.toList b.toList

/-- FEEL equality: numbers by value, then structural equality per type. Only
reached by `evalBin` for same-type or `null`-involving pairs; a cross-type pair
here is `false`. -/
def feelEq : Val → Val → Bool
  | .num a, .num b => a == b
  | .str a, .str b => a == b
  | .bool a, .bool b => a == b
  | .null, .null => true
  | _, _ => false

/-- Total order on comparable pairs (two numbers or two strings); `none` marks
an incomparable pair, which the caller turns into `null`. -/
def cmpVals : Val → Val → Option Ordering
  | .num a, .num b => some (compare a b)
  | .str a, .str b => some (strCompare a b)
  | _, _ => none

/-- The boolean a value holds, if any (FEEL never coerces). -/
def asBool : Val → Option Bool
  | .bool b => some b
  | _ => none

/-- Three-valued `and` on already-evaluated operands (Rust `ternary_and`). -/
def ternaryAnd (l r : Val) : Outcome :=
  match asBool l, asBool r with
  | some false, _ => .ok (.bool false)
  | _, some false => .ok (.bool false)
  | some true, some true => .ok (.bool true)
  | _, _ => .ok .null

/-- Three-valued `or` on already-evaluated operands (Rust `ternary_or`). -/
def ternaryOr (l r : Val) : Outcome :=
  match asBool l, asBool r with
  | some true, _ => .ok (.bool true)
  | _, some true => .ok (.bool true)
  | some false, some false => .ok (.bool false)
  | _, _ => .ok .null

/-- Applies a comparison predicate to two values; an incomparable pair is a
suppressed failure (`null`, #1392). -/
def cmpOp (l r : Val) (f : Ordering → Bool) : Outcome :=
  match cmpVals l r with
  | some o => .ok (.bool (f o))
  | none => .ok .null

/-- The FEEL type of a value, for the `=` / `!=` type check. -/
def Val.typeTag : Val → Nat
  | .null => 0
  | .bool _ => 1
  | .num _ => 2
  | .str _ => 3

/-- The `=` operator: `null` against anything is a boolean; two non-null values
of different types are a suppressed failure (`null`, #1394). -/
def eqOp (l r : Val) : Val :=
  if l == .null || r == .null || l.typeTag == r.typeTag then .bool (feelEq l r)
  else .null

/-- `!=`: the negation of `eqOp`, keeping its `null`. -/
def neOp (l r : Val) : Val :=
  match eqOp l r with
  | .bool b => .bool (!b)
  | v => v

/-- Evaluates a non-short-circuiting binary operator on two values. -/
def evalBin : BinOp → Val → Val → Outcome
  | .add, .num a, .num b => .ok (.num (a + b))
  | .add, .str a, .str b => .ok (.str (a ++ b))
  | .add, _, _ => .ok .null
  | .sub, .num a, .num b => .ok (.num (a - b))
  | .sub, _, _ => .ok .null
  | .mul, .num a, .num b => .ok (.num (a * b))
  | .mul, _, _ => .ok .null
  | .div, .num a, .num b => if b == 0 then .ok .null else .ok (.num (a / b))
  | .div, _, _ => .ok .null
  | .lt, a, b => cmpOp a b (· == .lt)
  | .le, a, b => cmpOp a b (· != .gt)
  | .gt, a, b => cmpOp a b (· == .gt)
  | .ge, a, b => cmpOp a b (· != .lt)
  | .eq, a, b => .ok (eqOp a b)
  | .ne, a, b => .ok (neOp a b)
  -- `and`/`or` short-circuit, so they are handled in `eval` and never reach here.
  | .and, _, _ => .err
  | .or, _, _ => .err

/-- The reference evaluator: a total function from context and expression to an
outcome. -/
def eval (ctx : Ctx) : Expr → Outcome
  | .nullLit => .ok .null
  | .boolLit b => .ok (.bool b)
  | .numLit n => .ok (.num (Int.ofNat n))
  | .strLit s => .ok (.str s)
  | .var name => .ok (ctx.lookup name)
  | .neg e =>
    match eval ctx e with
    | .ok (.num n) => .ok (.num (-n))
    | .ok _ => .ok .null
    | .err => .err
  | .lnot e =>
    match eval ctx e with
    | .ok (.bool b) => .ok (.bool (!b))
    | .ok _ => .ok .null
    | .err => .err
  | .cond c t e =>
    match eval ctx c with
    | .err => .err
    | .ok (.bool true) => eval ctx t
    | .ok _ => eval ctx e
  | .bin .and l r =>
    match eval ctx l with
    | .err => .err
    | .ok lv =>
      if lv == Val.bool false then .ok (.bool false)
      else match eval ctx r with
        | .err => .err
        | .ok rv => ternaryAnd lv rv
  | .bin .or l r =>
    match eval ctx l with
    | .err => .err
    | .ok lv =>
      if lv == Val.bool true then .ok (.bool true)
      else match eval ctx r with
        | .err => .err
        | .ok rv => ternaryOr lv rv
  | .bin op l r =>
    match eval ctx l, eval ctx r with
    | .err, _ => .err
    | _, .err => .err
    | .ok lv, .ok rv => evalBin op lv rv

end Feel
