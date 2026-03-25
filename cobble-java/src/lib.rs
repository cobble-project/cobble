#![allow(dead_code)]

use jni::JNIEnv;
use jni::objects::JClass;
use jni::sys::jstring;

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Utils_nativeVersionString0(
    env: JNIEnv,
    class: JClass,
) -> jstring {
    native_string(env, class, cobble::build_version_string())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Utils_nativeBuildCommitId0(
    env: JNIEnv,
    class: JClass,
) -> jstring {
    native_string(env, class, cobble::build_commit_short_id())
}

fn native_string(env: JNIEnv, _class: JClass, value: &'static str) -> jstring {
    match env.new_string(value) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}
