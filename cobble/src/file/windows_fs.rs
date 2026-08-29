use std::os::windows::ffi::OsStrExt;
use std::path::Path;

pub(crate) fn same_volume(source: &Path, destination: &Path) -> bool {
    volume_guid(source)
        .zip(volume_guid(destination))
        .is_some_and(|(source, destination)| source == destination)
}

fn volume_guid(path: &Path) -> Option<Vec<u16>> {
    const PATH_BUFFER_LEN: usize = 32_768;
    const VOLUME_GUID_BUFFER_LEN: usize = 64;

    let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut volume_path = vec![0; PATH_BUFFER_LEN];
    // SAFETY: Both buffers are valid for the lengths passed to the Win32 API and are
    // NUL-terminated/initialized as required.
    let found = unsafe {
        get_volume_path_name_w(
            path.as_ptr(),
            volume_path.as_mut_ptr(),
            volume_path.len() as u32,
        )
    };
    if found == 0 {
        return None;
    }

    let volume_path_len = volume_path.iter().position(|value| *value == 0)?;
    volume_path.truncate(volume_path_len + 1);
    let mut volume_guid = vec![0; VOLUME_GUID_BUFFER_LEN];
    // SAFETY: `volume_path` is NUL-terminated and `volume_guid` is writable for the
    // length supplied to the Win32 API.
    let found = unsafe {
        get_volume_name_for_volume_mount_point_w(
            volume_path.as_ptr(),
            volume_guid.as_mut_ptr(),
            volume_guid.len() as u32,
        )
    };
    if found == 0 {
        return None;
    }

    let volume_guid_len = volume_guid.iter().position(|value| *value == 0)?;
    volume_guid.truncate(volume_guid_len);
    Some(volume_guid)
}

#[link(name = "kernel32")]
unsafe extern "system" {
    #[link_name = "GetVolumePathNameW"]
    fn get_volume_path_name_w(
        file_name: *const u16,
        volume_path_name: *mut u16,
        buffer_length: u32,
    ) -> i32;
    #[link_name = "GetVolumeNameForVolumeMountPointW"]
    fn get_volume_name_for_volume_mount_point_w(
        volume_mount_point: *const u16,
        volume_name: *mut u16,
        buffer_length: u32,
    ) -> i32;
}
