//! The minimal condition expression language.
//!
//! Grammar (see `contracts/bt-config.schema.md`):
//!
//! ```text
//! expr     := or
//! or       := and ("or" and)*
//! and      := unary ("and" unary)*
//! unary    := "not" unary | primary
//! primary  := compare | contains | "(" expr ")"
//! compare  := value (">="|">"|"<="|"<"|"=="|"!=") value
//! contains := value ".contains(" value ")"
//! value    := envref | chainref | int | string
//! envref   := "env." DOTTED_IDENT
//! chainref := "cardano." IDENT
//! ```
//!
//! Conditions are parsed and type-validated at load time; a referenced-but-
//! undefined `env.X`, an unknown `cardano.X`, or a type mismatch is a hard
//! load-time error. Membership (`.contains`) is string containment over the
//! string-typed values (collection chain fields arrive when a condition needs
//! them).

use std::collections::BTreeMap;

use super::env::{ConsensusCtx, EnvValue, TickCtx, TreeContext, CHAIN_FIELDS};

/// A boolean precondition over a tree instantiation's per-tick context.
///
/// The generic tree stores conditions as `Box<dyn Condition<C>>`, so the
/// engine can evaluate a precondition without knowing the concrete context
/// type. `Debug + Send` so the compiled tree stays inspectable and movable
/// across tasks (mirroring the leaf-action contract).
pub trait Condition<C: TreeContext>: std::fmt::Debug + Send {
    /// Evaluate the predicate against this tick's context view.
    fn eval(&self, ctx: &C::View<'_>) -> bool;
}

/// The consensus binding: [`ConditionExpr`] evaluates against a [`TickCtx`].
/// Delegates to the inherent [`ConditionExpr::eval`] (inherent methods take
/// precedence over trait methods, so this does not recurse).
impl Condition<ConsensusCtx> for ConditionExpr {
    fn eval(&self, ctx: &TickCtx<'_>) -> bool {
        ConditionExpr::eval(self, ctx)
    }
}

/// A parsed, validated boolean predicate over env + chain state.
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionExpr {
    Compare {
        lhs: ValueRef,
        op: CompareOp,
        rhs: ValueRef,
    },
    Contains {
        container: ValueRef,
        item: ValueRef,
    },
    And(Vec<ConditionExpr>),
    Or(Vec<ConditionExpr>),
    Not(Box<ConditionExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Ge,
    Gt,
    Le,
    Lt,
    Eq,
    Ne,
}

/// A leaf value reference in a condition.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueRef {
    /// `env.<dotted>` — resolved against the merged env.
    Env(String),
    /// `cardano.<name>` — a [`NativeChainState`] field.
    Chain(String),
    LitU64(u64),
    LitStr(String),
}

/// The static type-class a value resolves to, used for validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeClass {
    Numeric,
    Str,
    Bool,
}

impl ConditionExpr {
    /// Parse an expression from its source text.
    pub fn parse(input: &str) -> Result<ConditionExpr, String> {
        let mut p = Parser::new(input);
        let expr = p.expr()?;
        p.skip_ws();
        if !p.at_end() {
            return Err(format!(
                "trailing input at column {}: {:?}",
                p.pos,
                &p.src[p.pos..]
            ));
        }
        Ok(expr)
    }

    /// Validate that every reference resolves and every comparison is well
    /// typed against the merged `env` and the known chain fields.
    pub fn validate(&self, env: &BTreeMap<String, EnvValue>) -> Result<(), String> {
        match self {
            ConditionExpr::Compare { lhs, op, rhs } => {
                let lc = type_class(lhs, env)?;
                let rc = type_class(rhs, env)?;
                if lc == TypeClass::Bool || rc == TypeClass::Bool {
                    return Err(format!(
                        "cannot compare bool values ({} {} {})",
                        describe(lhs),
                        op_str(*op),
                        describe(rhs)
                    ));
                }
                if lc != rc {
                    return Err(format!(
                        "type mismatch: {} ({:?}) {} {} ({:?})",
                        describe(lhs),
                        lc,
                        op_str(*op),
                        describe(rhs),
                        rc
                    ));
                }
                Ok(())
            }
            ConditionExpr::Contains { container, item } => {
                let cc = type_class(container, env)?;
                let ic = type_class(item, env)?;
                if cc != TypeClass::Str || ic != TypeClass::Str {
                    return Err(format!(
                        "contains requires string container and item: {}.contains({})",
                        describe(container),
                        describe(item)
                    ));
                }
                Ok(())
            }
            ConditionExpr::And(xs) | ConditionExpr::Or(xs) => {
                for x in xs {
                    x.validate(env)?;
                }
                Ok(())
            }
            ConditionExpr::Not(x) => x.validate(env),
        }
    }

    /// Evaluate the predicate in `ctx`. Assumes [`validate`](Self::validate)
    /// passed at load; a value that fails to resolve at tick time yields
    /// `false` rather than panicking (no panics in non-test code).
    pub fn eval(&self, ctx: &TickCtx) -> bool {
        match self {
            ConditionExpr::Compare { lhs, op, rhs } => {
                if let (Some(a), Some(b)) = (value_number(lhs, ctx), value_number(rhs, ctx)) {
                    return apply_num(*op, a, b);
                }
                if let (Some(a), Some(b)) = (value_string(lhs, ctx), value_string(rhs, ctx)) {
                    return apply_ord(*op, a.as_str(), b.as_str());
                }
                false
            }
            ConditionExpr::Contains { container, item } => {
                match (value_string(container, ctx), value_string(item, ctx)) {
                    (Some(c), Some(i)) => c.contains(&i),
                    _ => false,
                }
            }
            ConditionExpr::And(xs) => xs.iter().all(|x| x.eval(ctx)),
            ConditionExpr::Or(xs) => xs.iter().any(|x| x.eval(ctx)),
            ConditionExpr::Not(x) => !x.eval(ctx),
        }
    }
}

fn type_class(v: &ValueRef, env: &BTreeMap<String, EnvValue>) -> Result<TypeClass, String> {
    match v {
        ValueRef::LitU64(_) => Ok(TypeClass::Numeric),
        ValueRef::LitStr(_) => Ok(TypeClass::Str),
        ValueRef::Chain(name) => {
            if CHAIN_FIELDS.contains(&name.as_str()) {
                Ok(TypeClass::Numeric)
            } else {
                Err(format!("unknown chain field cardano.{name}"))
            }
        }
        ValueRef::Env(name) => match env.get(name) {
            None => Err(format!("undefined env reference env.{name}")),
            Some(EnvValue::Str(_)) => Ok(TypeClass::Str),
            Some(EnvValue::Bool(_)) => Ok(TypeClass::Bool),
            Some(ev) if ev.is_numeric() => Ok(TypeClass::Numeric),
            Some(ev) => Err(format!(
                "unsupported env type for env.{name}: {}",
                ev.type_name()
            )),
        },
    }
}

fn describe(v: &ValueRef) -> String {
    match v {
        ValueRef::Env(n) => format!("env.{n}"),
        ValueRef::Chain(n) => format!("cardano.{n}"),
        ValueRef::LitU64(n) => n.to_string(),
        ValueRef::LitStr(s) => format!("{s:?}"),
    }
}

fn op_str(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Ge => ">=",
        CompareOp::Gt => ">",
        CompareOp::Le => "<=",
        CompareOp::Lt => "<",
        CompareOp::Eq => "==",
        CompareOp::Ne => "!=",
    }
}

fn value_number(v: &ValueRef, ctx: &TickCtx) -> Option<f64> {
    match v {
        ValueRef::LitU64(n) => Some(*n as f64),
        ValueRef::Chain(name) => ctx.state.numeric_field(name).map(|n| n as f64),
        ValueRef::Env(name) => ctx.env.get(name).and_then(EnvValue::as_number),
        ValueRef::LitStr(_) => None,
    }
}

fn value_string(v: &ValueRef, ctx: &TickCtx) -> Option<String> {
    match v {
        ValueRef::LitStr(s) => Some(s.clone()),
        ValueRef::Env(name) => ctx
            .env
            .get(name)
            .and_then(EnvValue::as_str)
            .map(str::to_owned),
        _ => None,
    }
}

fn apply_num(op: CompareOp, a: f64, b: f64) -> bool {
    match op {
        CompareOp::Ge => a >= b,
        CompareOp::Gt => a > b,
        CompareOp::Le => a <= b,
        CompareOp::Lt => a < b,
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
    }
}

fn apply_ord(op: CompareOp, a: &str, b: &str) -> bool {
    match op {
        CompareOp::Ge => a >= b,
        CompareOp::Gt => a > b,
        CompareOp::Le => a <= b,
        CompareOp::Lt => a < b,
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
    }
}

/// Recursive-descent parser over the source bytes.
struct Parser<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            bytes: src.as_bytes(),
            pos: 0,
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn rest(&self) -> &str {
        &self.src[self.pos..]
    }

    /// Match a keyword honouring word boundaries (so `order` is not `or`).
    fn peek_word(&self, w: &str) -> bool {
        let r = self.rest();
        if let Some(after) = r.strip_prefix(w) {
            match after.bytes().next() {
                Some(c) => !is_ident_byte(c),
                None => true,
            }
        } else {
            false
        }
    }

    fn eat_word(&mut self, w: &str) -> bool {
        self.skip_ws();
        if self.peek_word(w) {
            self.pos += w.len();
            true
        } else {
            false
        }
    }

    fn expr(&mut self) -> Result<ConditionExpr, String> {
        self.or()
    }

    fn or(&mut self) -> Result<ConditionExpr, String> {
        let mut terms = vec![self.and()?];
        while self.eat_word("or") {
            terms.push(self.and()?);
        }
        Ok(if terms.len() == 1 {
            terms.pop().expect("len checked == 1")
        } else {
            ConditionExpr::Or(terms)
        })
    }

    fn and(&mut self) -> Result<ConditionExpr, String> {
        let mut terms = vec![self.unary()?];
        while self.eat_word("and") {
            terms.push(self.unary()?);
        }
        Ok(if terms.len() == 1 {
            terms.pop().expect("len checked == 1")
        } else {
            ConditionExpr::And(terms)
        })
    }

    fn unary(&mut self) -> Result<ConditionExpr, String> {
        if self.eat_word("not") {
            return Ok(ConditionExpr::Not(Box::new(self.unary()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<ConditionExpr, String> {
        self.skip_ws();
        if self.peek() == Some(b'(') {
            self.pos += 1;
            let e = self.expr()?;
            self.skip_ws();
            if self.peek() != Some(b')') {
                return Err(format!("expected ')' at column {}", self.pos));
            }
            self.pos += 1;
            return Ok(e);
        }

        let lhs = self.value()?;
        self.skip_ws();

        if self.rest().starts_with(".contains(") {
            self.pos += ".contains(".len();
            let item = self.value()?;
            self.skip_ws();
            if self.peek() != Some(b')') {
                return Err(format!(
                    "expected ')' to close .contains at column {}",
                    self.pos
                ));
            }
            self.pos += 1;
            return Ok(ConditionExpr::Contains {
                container: lhs,
                item,
            });
        }

        let op = self.compare_op()?;
        let rhs = self.value()?;
        Ok(ConditionExpr::Compare { lhs, op, rhs })
    }

    fn compare_op(&mut self) -> Result<CompareOp, String> {
        self.skip_ws();
        let r = self.rest();
        // Two-char operators first.
        for (tok, op) in [
            (">=", CompareOp::Ge),
            ("<=", CompareOp::Le),
            ("==", CompareOp::Eq),
            ("!=", CompareOp::Ne),
        ] {
            if r.starts_with(tok) {
                self.pos += 2;
                return Ok(op);
            }
        }
        for (tok, op) in [(">", CompareOp::Gt), ("<", CompareOp::Lt)] {
            if r.starts_with(tok) {
                self.pos += 1;
                return Ok(op);
            }
        }
        Err(format!(
            "expected a comparison operator or .contains at column {}",
            self.pos
        ))
    }

    fn value(&mut self) -> Result<ValueRef, String> {
        self.skip_ws();
        match self.peek() {
            Some(b'"') => self.string_lit(),
            Some(c) if c.is_ascii_digit() => self.int_lit(),
            Some(_) if self.rest().starts_with("env.") => {
                self.pos += "env.".len();
                let name = self.dotted_ident()?;
                Ok(ValueRef::Env(name))
            }
            Some(_) if self.rest().starts_with("cardano.") => {
                self.pos += "cardano.".len();
                let name = self.ident()?;
                Ok(ValueRef::Chain(name))
            }
            _ => Err(format!("expected a value at column {}", self.pos)),
        }
    }

    fn string_lit(&mut self) -> Result<ValueRef, String> {
        // Opening quote already peeked.
        self.pos += 1;
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b'"' {
                let s = self.src[start..self.pos].to_string();
                self.pos += 1;
                return Ok(ValueRef::LitStr(s));
            }
            self.pos += 1;
        }
        Err("unterminated string literal".to_string())
    }

    fn int_lit(&mut self) -> Result<ValueRef, String> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }
        let text = &self.src[start..self.pos];
        text.parse::<u64>()
            .map(ValueRef::LitU64)
            .map_err(|e| format!("invalid integer {text:?}: {e}"))
    }

    /// Read an identifier (letters, digits, `_`); no dots.
    fn ident(&mut self) -> Result<String, String> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if is_ident_byte(c) {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(format!("expected an identifier at column {}", self.pos));
        }
        Ok(self.src[start..self.pos].to_string())
    }

    /// Read a dotted identifier (`a.b.c`), stopping before a `.contains(`
    /// operator so `env.x.contains(...)` parses the membership form.
    fn dotted_ident(&mut self) -> Result<String, String> {
        let start = self.pos;
        // First segment is mandatory.
        self.ident()?;
        loop {
            if self.rest().starts_with(".contains(") {
                break;
            }
            if self.peek() == Some(b'.')
                && self
                    .bytes
                    .get(self.pos + 1)
                    .is_some_and(|c| is_ident_byte(*c))
            {
                self.pos += 1; // consume '.'
                self.ident()?;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(format!("expected an identifier at column {}", self.pos));
        }
        Ok(self.src[start..self.pos].to_string())
    }
}

fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

#[cfg(test)]
mod tests {
    use super::super::env::NativeChainState;
    use super::*;

    fn env_with(pairs: &[(&str, EnvValue)]) -> BTreeMap<String, EnvValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn ctx<'a>(env: &'a super::super::env::DynamicEnv, state: &'a NativeChainState) -> TickCtx<'a> {
        TickCtx {
            env,
            state,
            seed: 0,
            action_params: None,
        }
    }

    #[test]
    fn parses_comparison_with_env_and_chain_refs() {
        let e = ConditionExpr::parse("cardano.current_slot >= env.trigger_slot").unwrap();
        assert_eq!(
            e,
            ConditionExpr::Compare {
                lhs: ValueRef::Chain("current_slot".into()),
                op: CompareOp::Ge,
                rhs: ValueRef::Env("trigger_slot".into()),
            }
        );
    }

    #[test]
    fn parses_all_six_operators() {
        for (s, op) in [
            (">=", CompareOp::Ge),
            (">", CompareOp::Gt),
            ("<=", CompareOp::Le),
            ("<", CompareOp::Lt),
            ("==", CompareOp::Eq),
            ("!=", CompareOp::Ne),
        ] {
            let e = ConditionExpr::parse(&format!("cardano.current_slot {s} 10")).unwrap();
            match e {
                ConditionExpr::Compare { op: got, .. } => assert_eq!(got, op),
                other => panic!("expected compare, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_boolean_combinators_with_precedence() {
        // `not` binds tighter than `and`, which binds tighter than `or`.
        let e = ConditionExpr::parse(
            "cardano.current_slot >= 10 and not cardano.current_epoch == 0 or cardano.mempool_tx_count > 5",
        )
        .unwrap();
        match e {
            ConditionExpr::Or(terms) => {
                assert_eq!(terms.len(), 2);
                assert!(matches!(terms[0], ConditionExpr::And(_)));
            }
            other => panic!("expected top-level Or, got {other:?}"),
        }
    }

    #[test]
    fn parses_parenthesised_grouping() {
        let e = ConditionExpr::parse(
            "(cardano.current_slot >= 10 or cardano.current_epoch == 0) and cardano.mempool_tx_count > 5",
        )
        .unwrap();
        assert!(matches!(e, ConditionExpr::And(_)));
    }

    #[test]
    fn parses_contains_over_env_strings() {
        let e = ConditionExpr::parse("env.peers.contains(env.target)").unwrap();
        assert_eq!(
            e,
            ConditionExpr::Contains {
                container: ValueRef::Env("peers".into()),
                item: ValueRef::Env("target".into()),
            }
        );
    }

    #[test]
    fn parses_dotted_env_idents() {
        let e = ConditionExpr::parse("env.network_shape.packet_delay >= 1").unwrap();
        match e {
            ConditionExpr::Compare { lhs, .. } => {
                assert_eq!(lhs, ValueRef::Env("network_shape.packet_delay".into()))
            }
            other => panic!("expected compare, got {other:?}"),
        }
    }

    #[test]
    fn rejects_trailing_garbage() {
        assert!(ConditionExpr::parse("cardano.current_slot >= 10 garbage").is_err());
    }

    #[test]
    fn rejects_bare_value_without_operator() {
        assert!(ConditionExpr::parse("cardano.current_slot").is_err());
    }

    #[test]
    fn validate_rejects_undefined_env_ref() {
        let e = ConditionExpr::parse("cardano.current_slot >= env.nope").unwrap();
        let err = e.validate(&env_with(&[])).unwrap_err();
        assert!(err.contains("undefined env reference env.nope"), "{err}");
    }

    #[test]
    fn validate_rejects_unknown_chain_field() {
        let e = ConditionExpr::parse("cardano.height >= 10").unwrap();
        let err = e.validate(&env_with(&[])).unwrap_err();
        assert!(err.contains("unknown chain field cardano.height"), "{err}");
    }

    #[test]
    fn validate_rejects_type_mismatch() {
        let env = env_with(&[("name", EnvValue::Str("pool".into()))]);
        let e = ConditionExpr::parse("cardano.current_slot >= env.name").unwrap();
        let err = e.validate(&env).unwrap_err();
        assert!(err.contains("type mismatch"), "{err}");
    }

    #[test]
    fn validate_accepts_well_typed_numeric_compare() {
        let env = env_with(&[("trigger_slot", EnvValue::U64(10))]);
        let e = ConditionExpr::parse("cardano.current_slot >= env.trigger_slot").unwrap();
        assert!(e.validate(&env).is_ok());
    }

    #[test]
    fn eval_numeric_threshold() {
        use super::super::env::DynamicEnv;
        let mut env = DynamicEnv::new();
        env.insert("trigger_slot", EnvValue::U64(100));
        let e = ConditionExpr::parse("cardano.current_slot >= env.trigger_slot").unwrap();

        let before = NativeChainState {
            current_slot: 99,
            ..Default::default()
        };
        let at = NativeChainState {
            current_slot: 100,
            ..Default::default()
        };
        assert!(!e.eval(&ctx(&env, &before)));
        assert!(e.eval(&ctx(&env, &at)));
    }

    #[test]
    fn eval_boolean_combinators() {
        use super::super::env::DynamicEnv;
        let env = DynamicEnv::new();
        let state = NativeChainState {
            current_slot: 50,
            current_epoch: 2,
            mempool_tx_count: 0,
        };
        let e = ConditionExpr::parse(
            "cardano.current_slot >= 10 and (cardano.current_epoch == 2 or cardano.mempool_tx_count > 0)",
        )
        .unwrap();
        assert!(e.eval(&ctx(&env, &state)));

        let e2 = ConditionExpr::parse("not cardano.current_slot >= 10").unwrap();
        assert!(!e2.eval(&ctx(&env, &state)));
    }

    #[test]
    fn eval_contains_string_membership() {
        use super::super::env::DynamicEnv;
        let mut env = DynamicEnv::new();
        env.insert("peers", EnvValue::Str("a,b,c".into()));
        env.insert("target", EnvValue::Str("b".into()));
        env.insert("absent", EnvValue::Str("z".into()));
        let state = NativeChainState::default();

        let hit = ConditionExpr::parse("env.peers.contains(env.target)").unwrap();
        let miss = ConditionExpr::parse("env.peers.contains(env.absent)").unwrap();
        assert!(hit.eval(&ctx(&env, &state)));
        assert!(!miss.eval(&ctx(&env, &state)));
    }
}
