//! Tiny integer expressions for pattern arguments.
//!
//! A value is a number, a numeric string, or `{{ expr }}` where `expr` uses
//! integers, parameter names, `+ - * /` and parentheses. No conditionals or
//! loops: anything richer belongs in a pattern.

use std::collections::BTreeMap;

use crate::error::{Error, Result};

/// Evaluate a YAML value to a non-negative integer.
pub fn eval(value: &serde_yaml::Value, params: &BTreeMap<String, i64>) -> Result<u64> {
    let result = match value {
        serde_yaml::Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| Error::Spec(format!("`{n}` is not an integer in range")))?,
        serde_yaml::Value::String(s) => eval_str(s, params)?,
        other => return Err(Error::Spec(format!("expected a number, got {other:?}"))),
    };
    u64::try_from(result).map_err(|_| Error::Spec(format!("value {result} is negative")))
}

fn eval_str(text: &str, params: &BTreeMap<String, i64>) -> Result<i64> {
    let trimmed = text.trim();
    let inner = match trimmed
        .strip_prefix("{{")
        .and_then(|rest| rest.strip_suffix("}}"))
    {
        Some(inner) => inner,
        // Outside `{{ }}` only a plain number is allowed.
        None => {
            return trimmed.parse().map_err(|_| {
                Error::Spec(format!(
                    "`{text}` is neither a number nor a `{{{{ expression }}}}`"
                ))
            })
        }
    };
    let tokens = tokenize(inner)?;
    let mut parser = Parser {
        tokens: &tokens,
        position: 0,
        params,
        depth: 0,
    };
    let value = parser.expression()?;
    if parser.position != tokens.len() {
        return Err(Error::Spec(format!(
            "trailing input in expression `{text}`"
        )));
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(i64),
    Name(String),
    Op(char),
}

fn tokenize(text: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while let Some(&c) = chars.get(i) {
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_digit() {
            let start = i;
            while chars.get(i).is_some_and(|d| d.is_ascii_digit()) {
                i += 1;
            }
            let digits: String = chars.get(start..i).unwrap_or_default().iter().collect();
            let number = digits
                .parse()
                .map_err(|_| Error::Spec(format!("number `{digits}` out of range")))?;
            tokens.push(Token::Number(number));
        } else if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while chars
                .get(i)
                .is_some_and(|d| d.is_ascii_alphanumeric() || *d == '_')
            {
                i += 1;
            }
            tokens.push(Token::Name(
                chars.get(start..i).unwrap_or_default().iter().collect(),
            ));
        } else if "+-*/()".contains(c) {
            tokens.push(Token::Op(c));
            i += 1;
        } else {
            return Err(Error::Spec(format!(
                "unexpected `{c}` in expression `{text}`"
            )));
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Token],
    position: usize,
    params: &'a BTreeMap<String, i64>,
    /// Current nesting of `(` and unary `-`, bounded so input cannot overflow the stack.
    depth: usize,
}

/// Deepest nesting an expression may have.
const MAX_DEPTH: usize = 64;

impl Parser<'_> {
    fn peek_op(&self) -> Option<char> {
        match self.tokens.get(self.position) {
            Some(Token::Op(c)) => Some(*c),
            _ => None,
        }
    }

    fn expression(&mut self) -> Result<i64> {
        let mut value = self.term()?;
        while let Some(op @ ('+' | '-')) = self.peek_op() {
            self.position += 1;
            let rhs = self.term()?;
            value = if op == '+' {
                value.checked_add(rhs)
            } else {
                value.checked_sub(rhs)
            }
            .ok_or_else(|| Error::Spec("arithmetic overflow".to_string()))?;
        }
        Ok(value)
    }

    fn term(&mut self) -> Result<i64> {
        let mut value = self.factor()?;
        while let Some(op @ ('*' | '/')) = self.peek_op() {
            self.position += 1;
            let rhs = self.factor()?;
            value = if op == '*' {
                value.checked_mul(rhs)
            } else {
                value.checked_div(rhs)
            }
            .ok_or_else(|| Error::Spec("arithmetic overflow or division by zero".to_string()))?;
        }
        Ok(value)
    }

    fn factor(&mut self) -> Result<i64> {
        if self.depth >= MAX_DEPTH {
            return Err(Error::Spec(format!(
                "expression nests deeper than {MAX_DEPTH}"
            )));
        }
        self.depth += 1;
        let value = self.primary();
        self.depth -= 1;
        value
    }

    fn primary(&mut self) -> Result<i64> {
        let token = self
            .tokens
            .get(self.position)
            .cloned()
            .ok_or_else(|| Error::Spec("expression ends early".to_string()))?;
        self.position += 1;
        match token {
            Token::Number(n) => Ok(n),
            Token::Name(name) => self
                .params
                .get(&name)
                .copied()
                .ok_or_else(|| Error::Spec(format!("unknown parameter `{name}`"))),
            Token::Op('(') => {
                let value = self.expression()?;
                if self.peek_op() != Some(')') {
                    return Err(Error::Spec("missing `)`".to_string()));
                }
                self.position += 1;
                Ok(value)
            }
            Token::Op('-') => self
                .factor()?
                .checked_neg()
                .ok_or_else(|| Error::Spec("arithmetic overflow".to_string())),
            Token::Op(c) => Err(Error::Spec(format!("unexpected `{c}`"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> BTreeMap<String, i64> {
        BTreeMap::from([("scale".to_string(), 10), ("pairs".to_string(), 3)])
    }

    #[test]
    fn deep_nesting_is_an_error_not_a_stack_overflow() {
        let text = format!("{{{{ {}1 }}}}", "-".repeat(20_000));
        assert!(eval_text(&text).is_err());
        let text = format!("{{{{ {}1{} }}}}", "(".repeat(20_000), ")".repeat(20_000));
        assert!(eval_text(&text).is_err());
    }

    #[test]
    fn negating_the_minimum_is_an_error_not_a_panic() {
        let params = BTreeMap::from([("m".to_string(), i64::MIN)]);
        let value = serde_yaml::Value::String("{{ -m }}".to_string());
        assert!(eval(&value, &params).is_err());
    }

    fn eval_text(text: &str) -> Result<u64> {
        eval(&serde_yaml::Value::String(text.to_string()), &params())
    }

    #[test]
    fn numbers_and_params() {
        assert_eq!(
            eval(&serde_yaml::Value::Number(7.into()), &params()).ok(),
            Some(7)
        );
        assert_eq!(eval_text("12").ok(), Some(12));
        assert_eq!(eval_text("{{ scale }}").ok(), Some(10));
        assert_eq!(eval_text("{{ pairs * scale + 1 }}").ok(), Some(31));
        assert_eq!(eval_text("{{ (pairs + 1) * 2 }}").ok(), Some(8));
    }

    #[test]
    fn errors() {
        assert!(eval_text("{{ nope }}").is_err());
        assert!(eval_text("{{ 1 - 5 }}").is_err());
        assert!(eval_text("{{ 4 / 0 }}").is_err());
        assert!(eval_text("{{ 1 + }}").is_err());
        // Expressions and names need `{{ }}`.
        assert!(eval_text("3 * 2").is_err());
        assert!(eval_text("scale").is_err());
    }
}
