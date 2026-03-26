use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jint, jlong, jstring};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Utils_versionString(env: JNIEnv, class: JClass) -> jstring {
    native_string(env, class, cobble::build_version_string())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Utils_buildCommitId(env: JNIEnv, class: JClass) -> jstring {
    native_string(env, class, cobble::build_commit_short_id())
}

fn native_string(env: JNIEnv, _class: JClass, value: &'static str) -> jstring {
    match env.new_string(value) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

pub(crate) fn decode_java_string(
    env: &mut JNIEnv,
    value: JString,
) -> std::result::Result<String, String> {
    env.get_string(&value)
        .map(|s| s.into())
        .map_err(|err| format!("invalid java string: {}", err))
}

pub(crate) fn decode_java_bytes(
    env: &mut JNIEnv,
    value: JByteArray,
) -> std::result::Result<Vec<u8>, String> {
    env.convert_byte_array(value)
        .map_err(|err| format!("invalid java byte array: {}", err))
}

pub(crate) fn decode_u16(name: &str, value: jint) -> std::result::Result<u16, String> {
    if value < 0 || value > u16::MAX as jint {
        return Err(format!("{} out of range: {}", name, value));
    }
    Ok(value as u16)
}

pub(crate) fn decode_u32(name: &str, value: jint) -> std::result::Result<u32, String> {
    if value < 0 {
        return Err(format!("{} out of range: {}", name, value));
    }
    Ok(value as u32)
}

pub(crate) fn decode_u64_from_jlong(name: &str, value: jlong) -> std::result::Result<u64, String> {
    if value < 0 {
        return Err(format!("{} out of range: {}", name, value));
    }
    Ok(value as u64)
}

pub(crate) fn decode_column_index(value: jint) -> std::result::Result<usize, String> {
    if value < 0 {
        return Err(format!("column out of range: {}", value));
    }
    Ok(value as usize)
}

pub(crate) fn throw_illegal_state(env: &mut JNIEnv, message: String) {
    let _ = env.throw_new("java/lang/IllegalStateException", message);
}

pub(crate) fn throw_illegal_argument(env: &mut JNIEnv, message: String) {
    let _ = env.throw_new("java/lang/IllegalArgumentException", message);
}

pub(crate) fn to_java_string_or_throw(env: &mut JNIEnv, value: String) -> jstring {
    match env.new_string(value) {
        Ok(s) => s.into_raw(),
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            std::ptr::null_mut()
        }
    }
}
