//! Similar module to `sqlx::ty_match` overridden so that we can allow array like types in
//! compile-time statements as well. Most of the code is identical to the original.

use std::{marker::PhantomData, rc::Rc, sync::Arc};

use sqlx_exasol_impl::types::ExaIter;

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

impl<T, U> WrapSameExt for WrapSame<T, Option<U>> {
    type Wrapped = Option<T>;
}

impl<T, U> WrapSameExt for &'_ WrapSame<T, U> {
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

// ##############################
// ######## BEGIN CUSTOM ########
// ##############################

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<&'a [T]>> {
    type Matched = Option<&'a T>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a T, &'a [T]> {
    type Matched = &'a T;
}

impl<'a, T, const N: usize> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<[T; N]>> {
    type Matched = Option<&'a T>;
}

impl<'a, T, const N: usize> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<&'a [T; N]>> {
    type Matched = Option<&'a T>;
}

impl<T, const N: usize> MatchBorrowExt for MatchBorrow<T, [T; N]> {
    type Matched = T;
}

impl<'a, T, const N: usize> MatchBorrowExt for MatchBorrow<&'a T, [T; N]> {
    type Matched = &'a T;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<Vec<T>>> {
    type Matched = Option<&'a T>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<&'a Vec<T>>> {
    type Matched = Option<&'a T>;
}

impl<T> MatchBorrowExt for MatchBorrow<T, Vec<T>> {
    type Matched = T;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a T, Vec<T>> {
    type Matched = &'a T;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<Box<[T]>>> {
    type Matched = Option<&'a T>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<&'a Box<[T]>>> {
    type Matched = Option<&'a T>;
}

impl<T> MatchBorrowExt for MatchBorrow<T, Box<[T]>> {
    type Matched = T;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a T, Box<[T]>> {
    type Matched = &'a T;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<Rc<[T]>>> {
    type Matched = Option<&'a T>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<&'a Rc<[T]>>> {
    type Matched = Option<&'a T>;
}

impl<T> MatchBorrowExt for MatchBorrow<T, Rc<[T]>> {
    type Matched = T;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a T, Rc<[T]>> {
    type Matched = &'a T;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<Arc<[T]>>> {
    type Matched = Option<&'a T>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<Option<&'a T>, Option<&'a Arc<[T]>>> {
    type Matched = Option<&'a T>;
}

impl<T> MatchBorrowExt for MatchBorrow<T, Arc<[T]>> {
    type Matched = T;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a T, Arc<[T]>> {
    type Matched = &'a T;
}

impl<'i, I, T> MatchBorrowExt for MatchBorrow<T, ExaIter<'i, I, Option<T>>>
where
    I: IntoIterator<Item = &'i Option<T>> + Clone,
{
    type Matched = T;
}

impl<'i, I, T> MatchBorrowExt for MatchBorrow<&'i T, ExaIter<'i, I, Option<T>>>
where
    I: IntoIterator<Item = &'i Option<T>> + Clone,
{
    type Matched = &'i T;
}

impl<'i, I, T> MatchBorrowExt for MatchBorrow<T, ExaIter<'i, I, T>>
where
    I: IntoIterator<Item = &'i T> + Clone,
{
    type Matched = T;
}

impl<'i, I, T> MatchBorrowExt for MatchBorrow<&'i T, ExaIter<'i, I, T>>
where
    I: IntoIterator<Item = &'i T> + Clone,
{
    type Matched = &'i T;
}

// ##############################
// ######### END CUSTOM #########
// ##############################

impl<'a> MatchBorrowExt for MatchBorrow<Option<&'a str>, Option<String>> {
    type Matched = Option<&'a str>;
}

impl<'a> MatchBorrowExt for MatchBorrow<Option<&'a str>, Option<&'a String>> {
    type Matched = Option<&'a str>;
}

impl<'a> MatchBorrowExt for MatchBorrow<&'a str, String> {
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

impl<T, U> MatchBorrowExt for &'_ MatchBorrow<T, U> {
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

        let (_, match_borrow) = MatchBorrow::new(&&0i64, &0i64);
        let _: i64 = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new(&0i64, &0i64);
        let _: i64 = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new(0i64, &0i64);
        let _: i64 = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new(0i64, &vec![0i64; 1]);
        let _: i64 = match_borrow.match_borrow();
    }
}
