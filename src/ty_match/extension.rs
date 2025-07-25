//! Extension module that houses the array-like types implementations.

use std::{rc::Rc, sync::Arc};

use crate::{
    ty_match::{MatchBorrow, MatchBorrowExt, WrapSame, WrapSameExt},
    types::ExaIter,
};

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in an slice of [`Option`].
impl<'a, T, U> WrapSameExt for WrapSame<T, &'a [Option<U>]>
where
    T: 'a,
{
    type Wrapped = &'a [Option<T>];
}

/// Immutable references have middle priorty in autoref specialization.
/// This will take precedence over the mutalbe reference implementation but not before the owned
/// type implementation.
///
/// It wraps the type in a slice.
impl<'a, T, U> WrapSameExt for &WrapSame<T, &'a [U]>
where
    T: 'a,
{
    type Wrapped = &'a [T];
}

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in a fixed size array of [`Option`].
impl<T, U, const N: usize> WrapSameExt for WrapSame<T, [Option<U>; N]> {
    type Wrapped = [Option<T>; N];
}

/// Immutable references have middle priorty in autoref specialization.
/// This will take precedence over the mutalbe reference implementation but not before the owned
/// type implementation.
///
/// It wraps the type in a fixed size array.
impl<T, U, const N: usize> WrapSameExt for &WrapSame<T, [U; N]> {
    type Wrapped = [T; N];
}

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in a [`Vec<Option>`].
impl<T, U> WrapSameExt for WrapSame<T, Vec<Option<U>>> {
    type Wrapped = Vec<Option<T>>;
}

/// Immutable references have middle priorty in autoref specialization.
/// This will take precedence over the mutalbe reference implementation but not before the owned
/// type implementation.
///
/// It wraps the type in a [`Vec<T>`].
impl<T, U> WrapSameExt for &WrapSame<T, Vec<U>> {
    type Wrapped = Vec<T>;
}

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in a [`Box<[Option]>`].
impl<T, U> WrapSameExt for WrapSame<T, Box<[Option<U>]>> {
    type Wrapped = Box<[Option<T>]>;
}

/// Immutable references have middle priorty in autoref specialization.
/// This will take precedence over the mutalbe reference implementation but not before the owned
/// type implementation.
///
/// It wraps the type in a [`Box<[T]>`].
impl<T, U> WrapSameExt for &WrapSame<T, Box<[U]>> {
    type Wrapped = Box<[T]>;
}

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in a [`Rc<[Option]>`].
impl<T, U> WrapSameExt for WrapSame<T, Rc<[Option<U>]>> {
    type Wrapped = Rc<[Option<T>]>;
}

/// Immutable references have middle priorty in autoref specialization.
/// This will take precedence over the mutalbe reference implementation but not before the owned
/// type implementation.
///
/// It wraps the type in a [`Rc<[T]>`].
impl<T, U> WrapSameExt for &WrapSame<T, Rc<[U]>> {
    type Wrapped = Rc<[T]>;
}

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in a [`Arc<[Option]>`].
impl<T, U> WrapSameExt for WrapSame<T, Arc<[Option<U>]>> {
    type Wrapped = Arc<[Option<T>]>;
}

/// Immutable references have middle priorty in autoref specialization.
/// This will take precedence over the mutalbe reference implementation but not before the owned
/// type implementation.
///
/// It wraps the type in a [`Arc<[T]>`].
impl<T, U> WrapSameExt for &WrapSame<T, Arc<[U]>> {
    type Wrapped = Arc<[T]>;
}

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in a [`ExaIter<'_, _, Option>`].
///
/// NOTE: While this is a bit nonsensical, it does the job. Obviously, the iterator item
/// won't match the wrapped type, but that's fine since the goal of wrapping a type is achieved.
impl<I, T, U> WrapSameExt for WrapSame<T, ExaIter<I, Option<U>>> {
    type Wrapped = ExaIter<I, Option<T>>;
}

/// Owned types have the highest priorty in autoref specialization.
/// This will take precedence if we're wrapping a type in a [`ExaIter<'_, _, &Option>`].
///
/// NOTE: While this is a bit nonsensical, it does the job. Obviously, the iterator item
/// won't match the wrapped type, but that's fine since the goal of wrapping a type is achieved.
impl<'i, I, T, U> WrapSameExt for WrapSame<T, ExaIter<I, &'i Option<U>>>
where
    T: 'i,
{
    type Wrapped = ExaIter<I, &'i Option<T>>;
}

/// Immutable references have middle priorty in autoref specialization.
/// This will take precedence over the mutalbe reference implementation but not before the owned
/// type implementation.
///
/// It wraps the type in a [`ExaIter<'_, _, T>`].
///
/// NOTE: While this is a bit nonsensical, it does the job. Obviously, the iterator item
/// won't match the wrapped type, but that's fine since the goal of wrapping a type is achieved.
impl<I, T, U> WrapSameExt for &WrapSame<T, ExaIter<I, U>> {
    type Wrapped = ExaIter<I, T>;
}

/// Blanket impl for array slices.
impl<'a, T, U> MatchBorrowExt for MatchBorrow<&'a [T], &'a [U]>
where
    MatchBorrow<T, U>: MatchBorrowExt,
{
    type Matched = &'a [<MatchBorrow<T, U> as MatchBorrowExt>::Matched];
}

/// Blanket impl for fixed size arrays.
impl<T, U, const N: usize> MatchBorrowExt for MatchBorrow<[T; N], [U; N]>
where
    MatchBorrow<T, U>: MatchBorrowExt,
{
    type Matched = [<MatchBorrow<T, U> as MatchBorrowExt>::Matched; N];
}

/// Blanket impl for [`Vec`].
impl<T, U> MatchBorrowExt for MatchBorrow<Vec<T>, Vec<U>>
where
    MatchBorrow<T, U>: MatchBorrowExt,
{
    type Matched = Vec<<MatchBorrow<T, U> as MatchBorrowExt>::Matched>;
}

/// Blanket impl for [`Box<[T]>`].
impl<T, U> MatchBorrowExt for MatchBorrow<Box<[T]>, Box<[U]>>
where
    MatchBorrow<T, U>: MatchBorrowExt,
{
    type Matched = Box<[<MatchBorrow<T, U> as MatchBorrowExt>::Matched]>;
}

/// Blanket impl for [`Rc<[T]>`].
impl<T, U> MatchBorrowExt for MatchBorrow<Rc<[T]>, Rc<[U]>>
where
    MatchBorrow<T, U>: MatchBorrowExt,
{
    type Matched = Rc<[<MatchBorrow<T, U> as MatchBorrowExt>::Matched]>;
}

/// Blanket impl for [`Arc<[T]>`].
impl<T, U> MatchBorrowExt for MatchBorrow<Arc<[T]>, Arc<[U]>>
where
    MatchBorrow<T, U>: MatchBorrowExt,
{
    type Matched = Arc<[<MatchBorrow<T, U> as MatchBorrowExt>::Matched]>;
}

/// Blanket impl for [`ExaIter<'_, _, T>`].
impl<I1, I2, T, U> MatchBorrowExt for MatchBorrow<ExaIter<I1, T>, ExaIter<I2, U>>
where
    MatchBorrow<T, U>: MatchBorrowExt,
{
    type Matched = ExaIter<I1, <MatchBorrow<T, U> as MatchBorrowExt>::Matched>;
}

// ###############################
// ######## Option Extras ########
// ###############################
impl<'a> MatchBorrowExt for MatchBorrow<&'a Option<&'a str>, &'a Option<String>> {
    type Matched = &'a Option<&'a str>;
}

impl<'a> MatchBorrowExt for MatchBorrow<&'a Option<&'a str>, &'a Option<&'a String>> {
    type Matched = &'a Option<&'a str>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a Option<&'a T>, &Option<T>> {
    type Matched = &'a Option<T>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a Option<&'a &'a T>, &'a Option<T>> {
    type Matched = &'a Option<T>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a Option<T>, &'a Option<&'a T>> {
    type Matched = &'a Option<T>;
}

impl<'a, T> MatchBorrowExt for MatchBorrow<&'a Option<T>, &'a Option<&'a &'a T>> {
    type Matched = &'a Option<T>;
}

#[test]
#[allow(clippy::useless_vec)]
fn test_wrap_same() {
    if false {
        let _: &[i32] = WrapSame::<i32, _>::new(&[true].as_slice()).wrap_same();
        let _: &[Option<i32>] = WrapSame::<i32, _>::new(&[Some(true)].as_slice()).wrap_same();

        let _: [i32; 10] = WrapSame::<i32, _>::new(&[true; 10]).wrap_same();
        let _: [Option<i32>; 10] = WrapSame::<i32, _>::new(&[Some(true); 10]).wrap_same();

        let _: Vec<i32> = WrapSame::<i32, _>::new(&vec![true]).wrap_same();
        let _: Vec<Option<i32>> = WrapSame::<i32, _>::new(&vec![Some(true)]).wrap_same();

        let _: Box<[i32]> = WrapSame::<i32, _>::new(&vec![true].into_boxed_slice()).wrap_same();
        let _: Box<[Option<i32>]> =
            WrapSame::<i32, _>::new(&vec![Some(true)].into_boxed_slice()).wrap_same();

        let rc: Rc<[bool]> = Rc::from(vec![true]);
        let rc_opt: Rc<[Option<bool>]> = Rc::from(vec![Some(true)]);
        let _: Rc<[i32]> = WrapSame::<i32, _>::new(&rc).wrap_same();
        let _: Rc<[Option<i32>]> = WrapSame::<i32, _>::new(&rc_opt).wrap_same();

        let arc: Arc<[bool]> = Arc::from(vec![true]);
        let arc_opt: Arc<[Option<bool>]> = Arc::from(vec![Some(true)]);
        let _: Arc<[i32]> = WrapSame::<i32, _>::new(&arc).wrap_same();
        let _: Arc<[Option<i32>]> = WrapSame::<i32, _>::new(&arc_opt).wrap_same();

        let base_iter = vec![true];
        let _: ExaIter<_, i32> =
            WrapSame::<i32, _>::new(&ExaIter::new(base_iter.iter().filter(|_| true))).wrap_same();
        let _: ExaIter<_, Option<i32>> =
            WrapSame::<i32, _>::new(&ExaIter::new(base_iter.iter().filter(|_| true).map(Some)))
                .wrap_same();
    }
}

#[test]
fn test_match_borrow() {
    if false {
        let str_array = [String::new()];
        let (_, match_borrow) = MatchBorrow::new([""].as_slice(), &str_array.as_slice());
        let _: &[&str] = match_borrow.match_borrow();

        let opt_str_array = [Some(String::new())];
        let (_, match_borrow) = MatchBorrow::new([Some("")].as_slice(), &opt_str_array.as_slice());
        let _: &[Option<&str>] = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new([""], &str_array);
        let _: [&str; 1] = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new([Some("")], &opt_str_array);
        let _: [Option<&str>; 1] = match_borrow.match_borrow();

        let str_vec = vec![String::new()];
        let (_, match_borrow) = MatchBorrow::new(vec![""], &str_vec);
        let _: Vec<&str> = match_borrow.match_borrow();

        let opt_str_vec = vec![Some(String::new())];
        let (_, match_borrow) = MatchBorrow::new(vec![Some("")], &opt_str_vec);
        let _: Vec<Option<&str>> = match_borrow.match_borrow();

        let (_, match_borrow) =
            MatchBorrow::new(vec![""].into_boxed_slice(), &str_vec.into_boxed_slice());
        let _: Box<[&str]> = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new(
            vec![Some("")].into_boxed_slice(),
            &opt_str_vec.into_boxed_slice(),
        );
        let _: Box<[Option<&str>]> = match_borrow.match_borrow();

        let data = [String::new(), String::new(), String::new()];

        let t = ExaIter::new(data.iter().filter(|v| v.is_empty()));
        let u = ExaIter::new(data.iter().filter(|v| v.is_empty()));
        let (_, match_borrow) = MatchBorrow::new(t, &u);
        let _: ExaIter<_, &String> = match_borrow.match_borrow();

        let t = ExaIter::new(data.iter().filter(|v| v.is_empty()).map(|s| s.as_str()));
        let u = ExaIter::new(data.iter().filter(|v| v.is_empty()));
        let (_, match_borrow) = MatchBorrow::new(t, &u);
        let _: ExaIter<_, &str> = match_borrow.match_borrow();

        let t = ExaIter::new(
            data.iter()
                .filter(|v| v.is_empty())
                .map(|s| s.as_str())
                .map(Some),
        );
        let u = ExaIter::new(data.iter().filter(|v| v.is_empty()).map(Some));
        let (_, match_borrow) = MatchBorrow::new(t, &u);
        let _: ExaIter<_, Option<&str>> = match_borrow.match_borrow();

        // let (_, match_borrow) = MatchBorrow::new(Rc::from(vec![""]), &Rc::from(str_vec));
        // let _: Rc<[&str]> = match_borrow.match_borrow();

        // let (_, match_borrow) = MatchBorrow::new(Rc::from(vec![Some("")]),
        // &Rc::from(opt_str_vec)); let _: Rc<[Option<&str>]> = match_borrow.match_borrow();

        // let (_, match_borrow) = MatchBorrow::new(Arc::from(vec![""]), &Arc::from(str_vec));
        // let _: Arc<[&str]> = match_borrow.match_borrow();

        // let (_, match_borrow) =
        //     MatchBorrow::new(Arc::from(vec![Some("")]), &Arc::from(opt_str_vec));
        // let _: Arc<[Option<&str>]> = match_borrow.match_borrow();

        let int_array = [0];
        let (_, match_borrow) = MatchBorrow::new([0].as_slice(), &int_array.as_slice());
        let _: &[i32] = match_borrow.match_borrow();

        let opt_int_array = [Some(0)];
        let (_, match_borrow) = MatchBorrow::new([Some(0)].as_slice(), &opt_int_array.as_slice());
        let _: &[Option<i32>] = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new([0], &int_array);
        let _: [i32; 1] = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new([Some(0)], &opt_int_array);
        let _: [Option<i32>; 1] = match_borrow.match_borrow();

        let int_vec = vec![0];
        let (_, match_borrow) = MatchBorrow::new(vec![0], &int_vec);
        let _: Vec<i32> = match_borrow.match_borrow();

        let opt_int_vec = vec![Some(0)];
        let (_, match_borrow) = MatchBorrow::new(vec![Some(0)], &opt_int_vec);
        let _: Vec<Option<i32>> = match_borrow.match_borrow();

        let (_, match_borrow) =
            MatchBorrow::new(vec![0].into_boxed_slice(), &int_vec.into_boxed_slice());
        let _: Box<[i32]> = match_borrow.match_borrow();

        let (_, match_borrow) = MatchBorrow::new(
            vec![Some(0)].into_boxed_slice(),
            &opt_int_vec.into_boxed_slice(),
        );
        let _: Box<[Option<i32>]> = match_borrow.match_borrow();

        let data = [1, 2, 3];
        let t = ExaIter::new(data.iter().filter(|v| **v % 2 == 0));
        let u = ExaIter::new(data.iter().filter(|v| **v % 2 == 0));
        let (_, match_borrow) = MatchBorrow::new(t, &u);
        let _: ExaIter<_, &i32> = match_borrow.match_borrow();

        let t = ExaIter::new(data.iter().filter(|v| **v % 2 == 0).map(Some));
        let u = ExaIter::new(data.iter().filter(|v| **v % 2 == 0).map(Some));
        let (_, match_borrow) = MatchBorrow::new(t, &u);
        let _: ExaIter<_, Option<&i32>> = match_borrow.match_borrow();
    }
}
