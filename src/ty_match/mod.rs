//! Module similar to `sqlx::ty_match` overridden so that we can allow array like types in
//! compile-time statements as well. Most of the code in this parent module is the same as the
//! original.
//!
//! However, while the main idea behind the code is the same, we do extend the autoref
//! specialization to another level by making use of mutable reference implementations as well, in
//! addition to owned types and immutable reference implementation.

mod extension;

use std::marker::PhantomData;

#[allow(clippy::just_underscores_and_digits)]
pub fn same_type<T>(_1: &T, _2: &T) {}

pub struct WrapSame<T, U>(PhantomData<T>, PhantomData<U>);

impl<T, U> WrapSame<T, U> {
    pub fn new(_arg: &U) -> Self {
        WrapSame(PhantomData, PhantomData)
    }
}

pub trait WrapSameExt: Sized {
    type Wrapped;

    fn wrap_same(self) -> Self::Wrapped {
        panic!("only for type resolution")
    }
}

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in an [`Option`].
impl<T, U> WrapSameExt for WrapSame<T, Option<U>> {
    type Wrapped = Option<T>;
}

/// Mutable references have the lower priority in autoref specialization.
/// This is the blanket impl that returns the type as-is.
impl<T, U> WrapSameExt for &mut WrapSame<T, U> {
    type Wrapped = T;
}

pub struct MatchBorrow<T, U>(PhantomData<T>, PhantomData<U>);

impl<T, U> MatchBorrow<T, U> {
    pub fn new(t: T, _u: &U) -> (T, Self) {
        (t, MatchBorrow(PhantomData, PhantomData))
    }
}

pub trait MatchBorrowExt: Sized {
    type Matched;

    fn match_borrow(self) -> Self::Matched {
        panic!("only for type resolution")
    }
}

impl<'a> MatchBorrowExt for MatchBorrow<Option<&'a str>, Option<String>> {
    type Matched = Option<&'a str>;
}

impl<'a> MatchBorrowExt for MatchBorrow<Option<&'a str>, Option<&'a String>> {
    type Matched = Option<&'a str>;
}

impl<'a> MatchBorrowExt for MatchBorrow<&'a str, String> {
    type Matched = &'a str;
}

impl<'a> MatchBorrowExt for MatchBorrow<&'a str, &'a String> {
    type Matched = &'a str;
}

impl<T> MatchBorrowExt for MatchBorrow<&'_ T, T> {
    type Matched = T;
}

impl<T> MatchBorrowExt for MatchBorrow<&'_ &'_ T, T> {
    type Matched = T;
}

impl<T> MatchBorrowExt for MatchBorrow<T, &'_ T> {
    type Matched = T;
}

impl<T> MatchBorrowExt for MatchBorrow<T, &'_ &'_ T> {
    type Matched = T;
}

impl<T> MatchBorrowExt for MatchBorrow<Option<&'_ T>, Option<T>> {
    type Matched = Option<T>;
}

impl<T> MatchBorrowExt for MatchBorrow<Option<&'_ &'_ T>, Option<T>> {
    type Matched = Option<T>;
}

impl<T> MatchBorrowExt for MatchBorrow<Option<T>, Option<&'_ T>> {
    type Matched = Option<T>;
}

impl<T> MatchBorrowExt for MatchBorrow<Option<T>, Option<&'_ &'_ T>> {
    type Matched = Option<T>;
}

impl<T, U> MatchBorrowExt for &MatchBorrow<T, U> {
    type Matched = U;
}

pub fn conjure_value<T>() -> T {
    panic!()
}

pub fn dupe_value<T>(_t: &T) -> T {
    panic!()
}

#[test]
fn test_dupe_value() {
    let val = &(String::new(),);

    if false {
        let _: i32 = dupe_value(&0i32);
        let _: String = dupe_value(&String::new());
        let _: String = dupe_value(&val.0);
    }
}

#[test]
fn test_wrap_same() {
    if false {
        let _: i32 = WrapSame::<i32, _>::new(&0i32).wrap_same();
        let _: i32 = WrapSame::<i32, _>::new(&"hello, world!").wrap_same();
        let _: Option<i32> = WrapSame::<i32, _>::new(&Some(String::new())).wrap_same();
    }
}

#[test]
fn test_match_borrow() {
    if false {
        let (_, match_borrow) = MatchBorrow::new("", &String::new());
        let _: &str = match_borrow.match_borrow();

        let str = String::new();
        let (_, match_borrow) = MatchBorrow::new("", &&str);
        let _: &str = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new(&&0i64, &0i64);
        let _: i64 = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new(&0i64, &0i64);
        let _: i64 = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new(0i64, &0i64);
        let _: i64 = match_borrow.match_borrow();
    }
}
