use bytes::Bytes;
use cobble::{
    Error, File, FileSystem, ProcessFileSystemRegistry, ProcessFileSystemRequest, RandomAccessFile,
    Result, SequentialWriteFile, Url, clear_process_custom_file_system_registry,
    register_process_custom_file_system_registry,
};
use jni::JNIEnv;
use jni::JavaVM;
use jni::objects::{
    GlobalRef, JByteArray, JByteBuffer, JClass, JObject, JObjectArray, JString, JValue,
};
use jni::sys::jlong;
use std::collections::HashMap;
use std::sync::Arc;

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ProcessFileSystems_registerCustomRegistryInternal(
    mut env: JNIEnv,
    _class: JClass,
    registry: JObject,
) {
    if registry.is_null() {
        crate::util::throw_illegal_argument(&mut env, "registry must not be null".to_string());
        return;
    }
    let vm = match env.get_java_vm() {
        Ok(vm) => vm,
        Err(err) => {
            crate::util::throw_illegal_state(
                &mut env,
                format!("failed to obtain JavaVM for registry: {err}"),
            );
            return;
        }
    };
    let global = match env.new_global_ref(registry) {
        Ok(v) => v,
        Err(err) => {
            crate::util::throw_illegal_state(
                &mut env,
                format!("failed to create global ref for registry: {err}"),
            );
            return;
        }
    };
    register_process_custom_file_system_registry(Arc::new(JniProcessFileSystemRegistry {
        vm: Arc::new(vm),
        registry: global,
    }));
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ProcessFileSystems_clearCustomRegistryInternal(
    _env: JNIEnv,
    _class: JClass,
) {
    clear_process_custom_file_system_registry();
}

struct JniProcessFileSystemRegistry {
    vm: Arc<JavaVM>,
    registry: GlobalRef,
}

impl ProcessFileSystemRegistry for JniProcessFileSystemRegistry {
    fn try_init(&self, request: &ProcessFileSystemRequest) -> Result<Option<Arc<dyn FileSystem>>> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let request_obj = build_request_object(&mut env, request)?;
        let resolved = env
            .call_method(
                self.registry.as_obj(),
                "tryResolve",
                "(Lio/cobble/ProcessFileSystemRequest;)Lio/cobble/CustomFileSystem;",
                &[JValue::Object(&request_obj)],
            )
            .map_err(|err| Error::IoError(format!("custom registry tryResolve failed: {err}")))?
            .l()
            .map_err(|err| {
                Error::IoError(format!("custom registry tryResolve decode failed: {err}"))
            })?;
        if resolved.is_null() {
            return Ok(None);
        }
        let global = env.new_global_ref(&resolved).map_err(|err| {
            Error::IoError(format!("failed to pin resolved custom filesystem: {err}"))
        })?;
        Ok(Some(Arc::new(JniCustomFileSystem {
            vm: self.vm.clone(),
            object: global,
        })))
    }
}

struct JniCustomFileSystem {
    vm: Arc<JavaVM>,
    object: GlobalRef,
}

impl Drop for JniCustomFileSystem {
    fn drop(&mut self) {
        let _ = call_void_no_args(self.vm.as_ref(), self.object.as_obj(), "close");
    }
}

impl FileSystem for JniCustomFileSystem {
    fn init(
        _url: &Url,
        _access_id: Option<String>,
        _access_key: Option<String>,
        _custom_options: Option<HashMap<String, String>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Err(Error::FileSystemError(
            "custom JNI filesystem should be created by process registry".to_string(),
        ))
    }

    fn create_dir(&self, path: &str) -> Result<()> {
        call_void_with_string_arg(self.vm.as_ref(), self.object.as_obj(), "createDir", path)
    }

    fn exists(&self, path: &str) -> Result<bool> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let jpath = env
            .new_string(path)
            .map_err(|err| Error::IoError(format!("failed to encode path string: {err}")))?;
        env.call_method(
            self.object.as_obj(),
            "exists",
            "(Ljava/lang/String;)Z",
            &[JValue::Object(&JObject::from(jpath))],
        )
        .map_err(|err| Error::IoError(format!("custom filesystem exists failed: {err}")))?
        .z()
        .map_err(|err| Error::IoError(format!("custom filesystem exists decode failed: {err}")))
    }

    fn delete(&self, path: &str) -> Result<()> {
        call_void_with_string_arg(self.vm.as_ref(), self.object.as_obj(), "delete", path)
    }

    fn delete_async(&self, path: &str) -> Result<()> {
        call_void_with_string_arg(self.vm.as_ref(), self.object.as_obj(), "deleteAsync", path)
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let from_obj = JObject::from(
            env.new_string(from)
                .map_err(|err| Error::IoError(format!("failed to encode source path: {err}")))?,
        );
        let to_obj = JObject::from(
            env.new_string(to)
                .map_err(|err| Error::IoError(format!("failed to encode target path: {err}")))?,
        );
        env.call_method(
            self.object.as_obj(),
            "rename",
            "(Ljava/lang/String;Ljava/lang/String;)V",
            &[JValue::Object(&from_obj), JValue::Object(&to_obj)],
        )
        .map_err(|err| Error::IoError(format!("custom filesystem rename failed: {err}")))?;
        Ok(())
    }

    fn list(&self, path: &str) -> Result<Vec<String>> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let jpath = env
            .new_string(path)
            .map_err(|err| Error::IoError(format!("failed to encode path string: {err}")))?;
        let array_obj = env
            .call_method(
                self.object.as_obj(),
                "list",
                "(Ljava/lang/String;)[Ljava/lang/String;",
                &[JValue::Object(&JObject::from(jpath))],
            )
            .map_err(|err| Error::IoError(format!("custom filesystem list failed: {err}")))?
            .l()
            .map_err(|err| {
                Error::IoError(format!("custom filesystem list decode failed: {err}"))
            })?;
        if array_obj.is_null() {
            return Ok(Vec::new());
        }
        let array = JObjectArray::from(array_obj);
        let len = env.get_array_length(&array).map_err(|err| {
            Error::IoError(format!("custom filesystem list length failed: {err}"))
        })?;
        let mut out = Vec::with_capacity(len as usize);
        for index in 0..len {
            let item = env.get_object_array_element(&array, index).map_err(|err| {
                Error::IoError(format!("custom filesystem list item failed: {err}"))
            })?;
            if item.is_null() {
                continue;
            }
            let item_string = JString::from(item);
            let value = env.get_string(&item_string).map_err(|err| {
                Error::IoError(format!("custom filesystem list decode failed: {err}"))
            })?;
            out.push(value.into());
        }
        Ok(out)
    }

    fn open_read(&self, path: &str) -> Result<Box<dyn RandomAccessFile>> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let jpath = env
            .new_string(path)
            .map_err(|err| Error::IoError(format!("failed to encode path string: {err}")))?;
        let file_obj = env
            .call_method(
                self.object.as_obj(),
                "openRead",
                "(Ljava/lang/String;)Lio/cobble/CustomRandomAccessFile;",
                &[JValue::Object(&JObject::from(jpath))],
            )
            .map_err(|err| Error::IoError(format!("custom filesystem openRead failed: {err}")))?
            .l()
            .map_err(|err| {
                Error::IoError(format!("custom filesystem openRead decode failed: {err}"))
            })?;
        if file_obj.is_null() {
            return Err(Error::IoError(
                "custom filesystem openRead returned null".to_string(),
            ));
        }
        let global = env.new_global_ref(&file_obj).map_err(|err| {
            Error::IoError(format!(
                "failed to pin custom random access file handle: {err}"
            ))
        })?;
        let direct_enabled = query_support_direct(&self.vm, &global)?;
        Ok(Box::new(JniCustomRandomAccessFile {
            vm: self.vm.clone(),
            object: global,
            direct_enabled,
        }))
    }

    fn open_write(&self, path: &str) -> Result<Box<dyn SequentialWriteFile>> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let jpath = env
            .new_string(path)
            .map_err(|err| Error::IoError(format!("failed to encode path string: {err}")))?;
        let file_obj = env
            .call_method(
                self.object.as_obj(),
                "openWrite",
                "(Ljava/lang/String;)Lio/cobble/CustomSequentialWriteFile;",
                &[JValue::Object(&JObject::from(jpath))],
            )
            .map_err(|err| Error::IoError(format!("custom filesystem openWrite failed: {err}")))?
            .l()
            .map_err(|err| {
                Error::IoError(format!("custom filesystem openWrite decode failed: {err}"))
            })?;
        if file_obj.is_null() {
            return Err(Error::IoError(
                "custom filesystem openWrite returned null".to_string(),
            ));
        }
        let global = env.new_global_ref(&file_obj).map_err(|err| {
            Error::IoError(format!(
                "failed to pin custom sequential write file handle: {err}"
            ))
        })?;
        let direct_enabled = query_support_direct(&self.vm, &global)?;
        Ok(Box::new(JniCustomSequentialWriteFile {
            vm: self.vm.clone(),
            object: global,
            size: 0,
            direct_enabled,
            direct_cache: None,
        }))
    }

    fn last_modified(&self, path: &str) -> Result<Option<u64>> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let jpath = env
            .new_string(path)
            .map_err(|err| Error::IoError(format!("failed to encode path string: {err}")))?;
        let value = env
            .call_method(
                self.object.as_obj(),
                "lastModified",
                "(Ljava/lang/String;)Ljava/lang/Long;",
                &[JValue::Object(&JObject::from(jpath))],
            )
            .map_err(|err| Error::IoError(format!("custom filesystem lastModified failed: {err}")))?
            .l()
            .map_err(|err| {
                Error::IoError(format!(
                    "custom filesystem lastModified decode failed: {err}"
                ))
            })?;
        if value.is_null() {
            return Ok(None);
        }
        let raw = env
            .call_method(value, "longValue", "()J", &[])
            .map_err(|err| Error::IoError(format!("custom filesystem longValue failed: {err}")))?
            .j()
            .map_err(|err| {
                Error::IoError(format!("custom filesystem longValue decode failed: {err}"))
            })?;
        if raw < 0 {
            return Err(Error::IoError(format!(
                "invalid negative lastModified: {raw}"
            )));
        }
        Ok(Some(raw as u64))
    }
}

struct JniCustomRandomAccessFile {
    vm: Arc<JavaVM>,
    object: GlobalRef,
    direct_enabled: bool,
}

impl Drop for JniCustomRandomAccessFile {
    fn drop(&mut self) {
        let _ = call_void_no_args(self.vm.as_ref(), self.object.as_obj(), "close");
    }
}

impl File for JniCustomRandomAccessFile {
    fn close(&mut self) -> Result<()> {
        call_void_no_args(self.vm.as_ref(), self.object.as_obj(), "close")
    }

    fn size(&self) -> usize {
        let mut env = match self.vm.attach_current_thread() {
            Ok(env) => env,
            Err(_) => return 0,
        };
        let value = match env.call_method(self.object.as_obj(), "size", "()J", &[]) {
            Ok(v) => v,
            Err(_) => return 0,
        };
        let raw = match value.j() {
            Ok(v) => v,
            Err(_) => return 0,
        };
        if raw < 0 { 0 } else { raw as usize }
    }
}

impl RandomAccessFile for JniCustomRandomAccessFile {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        if size > i32::MAX as usize {
            return Err(Error::IoError(format!(
                "requested read size is too large for JNI: {size}"
            )));
        }
        if self.direct_enabled {
            return self.read_at_direct(offset, size);
        }
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let array = env
            .call_method(
                self.object.as_obj(),
                "readAt",
                "(JI)[B",
                &[JValue::Long(offset as jlong), JValue::Int(size as i32)],
            )
            .map_err(|err| Error::IoError(format!("custom readAt call failed: {err}")))?
            .l()
            .map_err(|err| Error::IoError(format!("custom readAt decode failed: {err}")))?;
        if array.is_null() {
            return Err(Error::IoError("custom readAt returned null".to_string()));
        }
        let bytes_array = JByteArray::from(array);
        let bytes = env
            .convert_byte_array(bytes_array)
            .map_err(|err| Error::IoError(format!("custom readAt byte decode failed: {err}")))?;
        Ok(Bytes::from(bytes))
    }
}

impl JniCustomRandomAccessFile {
    fn read_at_direct(&self, offset: usize, size: usize) -> Result<Bytes> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let direct = env
            .call_method(
                self.object.as_obj(),
                "readAtDirect",
                "(JI)Ljava/nio/ByteBuffer;",
                &[JValue::Long(offset as jlong), JValue::Int(size as i32)],
            )
            .map_err(|err| Error::IoError(format!("custom readAtDirect call failed: {err}")))?
            .l()
            .map_err(|err| Error::IoError(format!("custom readAtDirect decode failed: {err}")))?;
        if direct.is_null() {
            return Err(Error::IoError(
                "custom readAtDirect returned null".to_string(),
            ));
        }
        let direct_buffer = JByteBuffer::from(direct);
        let addr = env
            .get_direct_buffer_address(&direct_buffer)
            .map_err(|err| Error::IoError(format!("custom readAtDirect address failed: {err}")))?;
        let len = env
            .call_method(direct_buffer, "remaining", "()I", &[])
            .map_err(|err| Error::IoError(format!("custom readAtDirect remaining failed: {err}")))?
            .i()
            .map_err(|err| {
                Error::IoError(format!(
                    "custom readAtDirect remaining decode failed: {err}"
                ))
            })?;
        if len < 0 {
            return Err(Error::IoError(format!(
                "custom readAtDirect returned negative remaining length: {len}"
            )));
        }
        let slice = unsafe { std::slice::from_raw_parts(addr, len as usize) };
        Ok(Bytes::copy_from_slice(slice))
    }
}

struct JniCustomSequentialWriteFile {
    vm: Arc<JavaVM>,
    object: GlobalRef,
    size: usize,
    direct_enabled: bool,
    direct_cache: Option<DirectWriteBuffer>,
}

impl Drop for JniCustomSequentialWriteFile {
    fn drop(&mut self) {
        let _ = call_void_no_args(self.vm.as_ref(), self.object.as_obj(), "close");
    }
}

impl File for JniCustomSequentialWriteFile {
    fn close(&mut self) -> Result<()> {
        call_void_no_args(self.vm.as_ref(), self.object.as_obj(), "close")
    }

    fn size(&self) -> usize {
        let mut env = match self.vm.attach_current_thread() {
            Ok(env) => env,
            Err(_) => return self.size,
        };
        let value = match env.call_method(self.object.as_obj(), "size", "()J", &[]) {
            Ok(v) => v,
            Err(_) => return self.size,
        };
        let raw = match value.j() {
            Ok(v) => v,
            Err(_) => return self.size,
        };
        if raw < 0 { self.size } else { raw as usize }
    }
}

impl SequentialWriteFile for JniCustomSequentialWriteFile {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        if self.direct_enabled {
            return self.write_direct(data);
        }
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        let payload = env
            .byte_array_from_slice(data)
            .map_err(|err| Error::IoError(format!("failed to encode write payload: {err}")))?;
        let written = env
            .call_method(
                self.object.as_obj(),
                "write",
                "([B)I",
                &[JValue::Object(&JObject::from(payload))],
            )
            .map_err(|err| Error::IoError(format!("custom write call failed: {err}")))?
            .i()
            .map_err(|err| Error::IoError(format!("custom write decode failed: {err}")))?;
        if written < 0 {
            return Err(Error::IoError(format!(
                "custom write returned negative length: {written}"
            )));
        }
        self.size = self.size.saturating_add(written as usize);
        Ok(written as usize)
    }
}

struct DirectWriteBuffer {
    storage: Vec<u8>,
    byte_buffer_ref: GlobalRef,
}

impl JniCustomSequentialWriteFile {
    fn write_direct(&mut self, data: &[u8]) -> Result<usize> {
        if data.len() > i32::MAX as usize {
            return Err(Error::IoError(format!(
                "requested write size is too large for direct JNI: {}",
                data.len()
            )));
        }
        let vm = Arc::clone(&self.vm);
        let mut env = vm
            .attach_current_thread()
            .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
        self.ensure_direct_cache(&mut env, data.len())?;
        let cache = self
            .direct_cache
            .as_mut()
            .ok_or_else(|| Error::IoError("direct cache is missing".to_string()))?;
        cache.storage[..data.len()].copy_from_slice(data);
        let local_buffer = env
            .new_local_ref(cache.byte_buffer_ref.as_obj())
            .map_err(|err| {
                Error::IoError(format!("failed to access cached direct buffer: {err}"))
            })?;
        let written = env
            .call_method(
                self.object.as_obj(),
                "writeDirect",
                "(Ljava/nio/ByteBuffer;I)I",
                &[
                    JValue::Object(&local_buffer),
                    JValue::Int(data.len() as i32),
                ],
            )
            .map_err(|err| Error::IoError(format!("custom writeDirect call failed: {err}")))?
            .i()
            .map_err(|err| Error::IoError(format!("custom writeDirect decode failed: {err}")))?;
        if written < 0 {
            return Err(Error::IoError(format!(
                "custom writeDirect returned negative length: {written}"
            )));
        }
        self.size = self.size.saturating_add(written as usize);
        Ok(written as usize)
    }

    fn ensure_direct_cache(&mut self, env: &mut JNIEnv, required: usize) -> Result<()> {
        let mut current_capacity = 0usize;
        if let Some(cache) = self.direct_cache.as_ref() {
            current_capacity = cache.storage.len();
            if current_capacity >= required {
                return Ok(());
            }
        }
        let capacity = required.max(current_capacity.saturating_mul(2)).max(1024);
        let mut storage = vec![0u8; capacity];
        let byte_buffer =
            unsafe { env.new_direct_byte_buffer(storage.as_mut_ptr(), storage.len()) }.map_err(
                |err| Error::IoError(format!("failed to allocate direct write buffer: {err}")),
            )?;
        let byte_buffer_ref = env
            .new_global_ref(byte_buffer)
            .map_err(|err| Error::IoError(format!("failed to pin direct write buffer: {err}")))?;
        self.direct_cache = Some(DirectWriteBuffer {
            storage,
            byte_buffer_ref,
        });
        Ok(())
    }
}

fn build_request_object<'local>(
    env: &mut JNIEnv<'local>,
    request: &ProcessFileSystemRequest,
) -> Result<JObject<'local>> {
    let map = new_java_map(env, request.custom_options.as_ref())?;
    let base_dir = new_java_optional_string(env, Some(&request.base_dir))?;
    let normalized = new_java_optional_string(env, request.normalized_base_dir.as_deref())?;
    let access_id = new_java_optional_string(env, request.access_id.as_deref())?;
    let secret_key = new_java_optional_string(env, request.secret_key.as_deref())?;
    let builtin_error = new_java_optional_string(env, Some(&request.builtin_error.to_string()))?;
    env.new_object(
        "io/cobble/ProcessFileSystemRequest",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/util/Map;Ljava/lang/String;)V",
        &[
            JValue::Object(&base_dir),
            JValue::Object(&normalized),
            JValue::Object(&access_id),
            JValue::Object(&secret_key),
            JValue::Object(&map),
            JValue::Object(&builtin_error),
        ],
    )
    .map_err(|err| Error::IoError(format!("failed to create ProcessFileSystemRequest: {err}")))
}

fn new_java_map<'local>(
    env: &mut JNIEnv<'local>,
    entries: Option<&HashMap<String, String>>,
) -> Result<JObject<'local>> {
    let map = env
        .new_object("java/util/HashMap", "()V", &[])
        .map_err(|err| Error::IoError(format!("failed to create HashMap: {err}")))?;
    if let Some(entries) = entries {
        for (key, value) in entries {
            let key_obj = JObject::from(
                env.new_string(key)
                    .map_err(|err| Error::IoError(format!("failed to encode map key: {err}")))?,
            );
            let value_obj = JObject::from(
                env.new_string(value)
                    .map_err(|err| Error::IoError(format!("failed to encode map value: {err}")))?,
            );
            env.call_method(
                &map,
                "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&key_obj), JValue::Object(&value_obj)],
            )
            .map_err(|err| {
                Error::IoError(format!("failed to put custom option map entry: {err}"))
            })?;
        }
    }
    Ok(map)
}

fn new_java_optional_string<'local>(
    env: &mut JNIEnv<'local>,
    value: Option<&str>,
) -> Result<JObject<'local>> {
    let Some(value) = value else {
        return Ok(JObject::null());
    };
    env.new_string(value)
        .map(JObject::from)
        .map_err(|err| Error::IoError(format!("failed to encode java string: {err}")))
}

fn call_void_with_string_arg(vm: &JavaVM, target: &JObject, method: &str, arg: &str) -> Result<()> {
    let mut env = vm
        .attach_current_thread()
        .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
    let value = JObject::from(
        env.new_string(arg)
            .map_err(|err| Error::IoError(format!("failed to encode argument string: {err}")))?,
    );
    env.call_method(
        target,
        method,
        "(Ljava/lang/String;)V",
        &[JValue::Object(&value)],
    )
    .map_err(|err| Error::IoError(format!("custom filesystem {method} failed: {err}")))?;
    Ok(())
}

fn call_void_no_args(vm: &JavaVM, target: &JObject, method: &str) -> Result<()> {
    let mut env = vm
        .attach_current_thread()
        .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
    env.call_method(target, method, "()V", &[])
        .map_err(|err| Error::IoError(format!("custom filesystem {method} failed: {err}")))?;
    Ok(())
}

fn query_support_direct(vm: &JavaVM, target: &GlobalRef) -> Result<bool> {
    let mut env = vm
        .attach_current_thread()
        .map_err(|err| Error::IoError(format!("failed to attach JVM thread: {err}")))?;
    env.call_method(target.as_obj(), "supportDirect", "()Z", &[])
        .map_err(|err| Error::IoError(format!("custom file supportDirect failed: {err}")))?
        .z()
        .map_err(|err| Error::IoError(format!("custom file supportDirect decode failed: {err}")))
}
