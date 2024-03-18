//! C wrappers

extern crate libc;
use libc::size_t;

use std::{
    ffi::{c_char, c_uchar, c_ulonglong, CStr, CString},
    mem::{forget, ManuallyDrop},
    ptr, slice,
};

use tokio::runtime::Runtime;

use crate::Hyperbee;

type RuntimePointer = *mut Runtime;
type HyperbeePointer = *mut Hyperbee;
type HbDiskPointer = HyperbeePointer;

/// Type of callback we pass results to
/// seq == 0 means the key was not found
pub type HbGetResultCallback = unsafe extern "C" fn(
    error_code: i64,
    error_message: *const c_char,
    seq: c_ulonglong,
    value_buff: *mut u8,
    value_len: size_t,
);

/// Take a Rust thing and create a pointer to pass C, and make rust "forget" about
/// it so the underlying memory is not freed
fn to_c<T>(obj: T) -> *mut T {
    Box::into_raw(Box::new(obj))
}

/// Take a pointer from C and create a Rust thing
fn from_c<T>(pointer: *mut T) -> ManuallyDrop<Box<T>> {
    ManuallyDrop::new(unsafe { Box::from_raw(pointer) })
}

#[no_mangle]
unsafe extern "C" fn deallocate_rust_string(ptr: *mut c_char) {
    drop(CString::from_raw(ptr));
}

/// This is intended for the C code to call for deallocating the
/// Rust-allocated u8 array.
/// https://stackoverflow.com/questions/39224904/how-to-expose-a-rust-vect-to-ffi/39270881#39270881
#[no_mangle]
unsafe extern "C" fn deallocate_rust_buffer(ptr: *mut u8, len: size_t) {
    drop(Vec::from_raw_parts(ptr, len, len));
}

pub fn string_from_c_str(c_str: *const c_char) -> Option<String> {
    if c_str.is_null() {
        return None;
    }
    unsafe { Some(CStr::from_ptr(c_str).to_string_lossy().into_owned()) }
}

/// Initialize the async runtime
#[no_mangle]
// TODO handle errors
pub extern "C" fn init_runtime() -> RuntimePointer {
    match Runtime::new() {
        Ok(rt) => to_c(rt),
        Err(_e) => {
            // TODO use tracing: println!("Error setting up runtime!: {}", e);
            ptr::null_mut()
        }
    }
}

/// Close async runtime
#[no_mangle]
pub extern "C" fn close_runtime(rt: RuntimePointer) {
    let rt = from_c(rt);
    drop(ManuallyDrop::<Box<Runtime>>::into_inner(rt));
}

#[no_mangle]
pub extern "C" fn close_hyperbee(hb: HbDiskPointer) {
    let hb = from_c(hb);
    drop(ManuallyDrop::<Box<Hyperbee>>::into_inner(hb));
}

pub fn c_vec_from_vec<T: std::fmt::Debug>(mut vec: Vec<T>) -> (*mut T, size_t) {
    vec.shrink_to_fit();
    assert!(vec.len() == vec.capacity());
    let ptr = vec.as_mut_ptr();
    let len = vec.len();
    forget(vec);
    (ptr, len)
}

/// Load a Hyperbee from a directory
#[no_mangle]
// TODO handle errors
pub extern "C" fn hyperbee_from_storage_directory(
    rt: RuntimePointer,
    storage_directory: *const c_char,
) -> HyperbeePointer {
    let rt = from_c(rt);
    let storage_directory = match string_from_c_str(storage_directory) {
        None => {
            // TODO use tracing: println!("No `storage_directory` provided to connect to");
            return ptr::null_mut();
        }
        Some(x) => x,
    };

    let hb = rt.block_on(async { Hyperbee::from_storage_dir(storage_directory).await });
    match hb {
        Ok(r) => to_c(r),
        Err(_) => ptr::null_mut(),
    }
}

fn vec_from_c_vec(bytes: *const c_uchar, bytes_length: size_t) -> Vec<u8> {
    let bytes = unsafe { slice::from_raw_parts(bytes, bytes_length) };
    Vec::from(bytes)
}

#[no_mangle]
pub extern "C" fn hb_get(
    rt: RuntimePointer,
    hb: HbDiskPointer,
    key_buff: *const c_uchar,
    key_size: size_t,
    callback: HbGetResultCallback,
) -> i32 {
    let rt = from_c(rt);
    let hb = from_c(hb);
    let rkey = vec_from_c_vec(key_buff, key_size);

    rt.spawn(async move {
        match hb.get(&rkey).await {
            Ok(Some((seq, Some(value)))) => {
                let (vec, size) = c_vec_from_vec(value);
                let no_error_msg = CString::new("").unwrap().into_raw();
                unsafe {
                    callback(0, no_error_msg, seq, vec, size);
                }
            }
            Ok(_) => todo!(),
            Err(_e) => todo!(),
        };
    });
    0
}
