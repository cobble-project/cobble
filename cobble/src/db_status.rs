use crate::error::{Error, Result};
use arc_swap::ArcSwapOption;
use log::warn;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

const STATE_INITIALIZING: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_CLOSING: u8 = 2;
const STATE_CLOSED: u8 = 3;
const STATE_ERROR: u8 = 4;

const ACCESS_OPEN: u8 = 0;
const ACCESS_EXCLUSIVE_REQUESTED: u8 = 1;
const ACCESS_EXCLUSIVE: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DbLifecycleState {
    Initializing,
    Open,
    Closing,
    Closed,
    Error,
}

impl DbLifecycleState {
    fn as_u8(self) -> u8 {
        match self {
            Self::Initializing => STATE_INITIALIZING,
            Self::Open => STATE_OPEN,
            Self::Closing => STATE_CLOSING,
            Self::Closed => STATE_CLOSED,
            Self::Error => STATE_ERROR,
        }
    }

    fn from_u8(raw: u8) -> Self {
        match raw {
            STATE_INITIALIZING => Self::Initializing,
            STATE_OPEN => Self::Open,
            STATE_CLOSING => Self::Closing,
            STATE_CLOSED => Self::Closed,
            STATE_ERROR => Self::Error,
            _ => Self::Error,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Initializing => "initializing",
            Self::Open => "open",
            Self::Closing => "closing",
            Self::Closed => "closed",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseTransition {
    Transitioned,
    AlreadyClosingOrClosed,
}

pub(crate) struct DbAccessGuard<'a> {
    lifecycle: &'a DbLifecycle,
}

impl Drop for DbAccessGuard<'_> {
    fn drop(&mut self) {
        self.lifecycle.release_access();
    }
}

pub(crate) struct OwnedDbAccessGuard {
    lifecycle: Arc<DbLifecycle>,
}

pub(crate) struct ExclusiveDbAccessGuard {
    lifecycle: Arc<DbLifecycle>,
}

impl Drop for ExclusiveDbAccessGuard {
    fn drop(&mut self) {
        self.lifecycle
            .access_mode
            .store(ACCESS_OPEN, Ordering::Release);
        self.lifecycle.release_access();
        self.lifecycle.exclusive_wait_condvar.notify_all();
    }
}

impl Drop for OwnedDbAccessGuard {
    fn drop(&mut self) {
        self.lifecycle.release_access();
    }
}

pub(crate) struct DbLifecycle {
    state: AtomicU8,
    access_mode: AtomicU8,
    active_accesses: AtomicUsize,
    error: ArcSwapOption<Error>,
    /// Condvars to notify when the lifecycle enters an error/closing state.
    error_notifiers: Mutex<Vec<Weak<Condvar>>>,
    access_wait_mutex: Mutex<()>,
    access_wait_condvar: Condvar,
    exclusive_wait_mutex: Mutex<()>,
    exclusive_wait_condvar: Condvar,
}

impl DbLifecycle {
    pub(crate) fn new_initializing() -> Self {
        Self {
            state: AtomicU8::new(STATE_INITIALIZING),
            access_mode: AtomicU8::new(ACCESS_OPEN),
            active_accesses: AtomicUsize::new(0),
            error: ArcSwapOption::empty(),
            error_notifiers: Mutex::new(Vec::new()),
            access_wait_mutex: Mutex::new(()),
            access_wait_condvar: Condvar::new(),
            exclusive_wait_mutex: Mutex::new(()),
            exclusive_wait_condvar: Condvar::new(),
        }
    }

    pub(crate) fn new_open() -> Self {
        Self {
            state: AtomicU8::new(STATE_OPEN),
            access_mode: AtomicU8::new(ACCESS_OPEN),
            active_accesses: AtomicUsize::new(0),
            error: ArcSwapOption::empty(),
            error_notifiers: Mutex::new(Vec::new()),
            access_wait_mutex: Mutex::new(()),
            access_wait_condvar: Condvar::new(),
            exclusive_wait_mutex: Mutex::new(()),
            exclusive_wait_condvar: Condvar::new(),
        }
    }

    #[inline]
    pub(crate) fn is_open_fast(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STATE_OPEN
    }

    pub(crate) fn state(&self) -> DbLifecycleState {
        DbLifecycleState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub(crate) fn mark_open(&self) -> Result<()> {
        loop {
            let current = self.state.load(Ordering::Acquire);
            match DbLifecycleState::from_u8(current) {
                DbLifecycleState::Initializing => {
                    if self
                        .state
                        .compare_exchange(current, STATE_OPEN, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.error.store(None);
                        return Ok(());
                    }
                }
                DbLifecycleState::Open => return Ok(()),
                DbLifecycleState::Error => return Err(self.error_or_invalid_state()),
                DbLifecycleState::Closing | DbLifecycleState::Closed => {
                    return Err(Error::InvalidState(format!(
                        "db cannot be opened from {} state",
                        DbLifecycleState::from_u8(current).as_str()
                    )));
                }
            }
        }
    }

    pub(crate) fn begin_close(&self) -> Result<CloseTransition> {
        loop {
            let current = self.state.load(Ordering::Acquire);
            match DbLifecycleState::from_u8(current) {
                DbLifecycleState::Open => {
                    if self
                        .state
                        .compare_exchange(
                            current,
                            STATE_CLOSING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.notify_error_watchers();
                        return Ok(CloseTransition::Transitioned);
                    }
                }
                DbLifecycleState::Initializing => {
                    return Err(Error::InvalidState("db is still initializing".to_string()));
                }
                DbLifecycleState::Closing | DbLifecycleState::Closed => {
                    return Ok(CloseTransition::AlreadyClosingOrClosed);
                }
                DbLifecycleState::Error => return Err(self.error_or_invalid_state()),
            }
        }
    }

    pub(crate) fn mark_closed(&self) {
        self.state.store(STATE_CLOSED, Ordering::Release);
    }

    pub(crate) fn mark_error(&self, err: Error) {
        self.error.store(Some(Arc::new(err)));
        self.state.store(STATE_ERROR, Ordering::Release);
        self.notify_error_watchers();
    }

    /// Registers a condvar to be notified when the lifecycle enters error or closing state.
    pub(crate) fn register_error_notifier(&self, condvar: &Arc<Condvar>) {
        let mut notifiers = self.error_notifiers.lock().unwrap();
        notifiers.push(Arc::downgrade(condvar));
    }

    fn notify_error_watchers(&self) {
        let notifiers = self.error_notifiers.lock().unwrap();
        for notifier in notifiers.iter() {
            if let Some(cv) = notifier.upgrade() {
                cv.notify_all();
            }
        }
    }

    pub(crate) fn error(&self) -> Option<Error> {
        (self.state() == DbLifecycleState::Error)
            .then(|| self.error.load_full().map(|err| err.as_ref().clone()))
            .flatten()
    }

    #[inline]
    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.is_open_fast() {
            return Ok(());
        }
        Err(self.error_or_invalid_state())
    }

    pub(crate) fn begin_access(&self) -> Result<DbAccessGuard<'_>> {
        self.try_begin_access()?;
        Ok(DbAccessGuard { lifecycle: self })
    }

    pub(crate) fn begin_owned_access(self: &Arc<Self>) -> Result<OwnedDbAccessGuard> {
        self.try_begin_access()?;
        Ok(OwnedDbAccessGuard {
            lifecycle: Arc::clone(self),
        })
    }

    pub(crate) fn begin_exclusive_access(self: &Arc<Self>) -> Result<ExclusiveDbAccessGuard> {
        let mut wait_guard = self.exclusive_wait_mutex.lock().unwrap();
        while self.access_mode.load(Ordering::Acquire) != ACCESS_OPEN {
            wait_guard = self.exclusive_wait_condvar.wait(wait_guard).unwrap();
        }
        self.ensure_open()?;
        // Keep one active-access sentinel from the first transition onward so close cannot miss
        // an exclusive operation between observing Open and entering its body.
        self.active_accesses.fetch_add(1, Ordering::AcqRel);
        self.access_mode
            .store(ACCESS_EXCLUSIVE_REQUESTED, Ordering::Release);
        drop(wait_guard);

        self.wait_for_other_accesses_to_drain();
        if !self.is_open_fast() {
            self.access_mode.store(ACCESS_OPEN, Ordering::Release);
            self.release_access();
            self.exclusive_wait_condvar.notify_all();
            return Err(self.error_or_invalid_state());
        }
        self.access_mode.store(ACCESS_EXCLUSIVE, Ordering::Release);
        Ok(ExclusiveDbAccessGuard {
            lifecycle: Arc::clone(self),
        })
    }

    fn try_begin_access(&self) -> Result<()> {
        loop {
            let current = self.state.load(Ordering::Acquire);
            if current != STATE_OPEN {
                return Err(self.error_or_invalid_state());
            }
            if self.access_mode.load(Ordering::Acquire) != ACCESS_OPEN {
                return Err(Error::InvalidState("db has exclusive access".to_string()));
            }
            self.active_accesses.fetch_add(1, Ordering::AcqRel);
            if self.state.load(Ordering::Acquire) == STATE_OPEN
                && self.access_mode.load(Ordering::Acquire) == ACCESS_OPEN
            {
                return Ok(());
            }
            self.release_access();
        }
    }

    pub(crate) fn wait_for_accesses_to_drain(&self) {
        if self.active_accesses.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut wait_guard = self.access_wait_mutex.lock().unwrap();
        let mut waited = 0u64;
        while self.active_accesses.load(Ordering::Acquire) != 0 {
            if waited >= 30_000 {
                warn!(
                    "waited at least 30 seconds to quit, possible block of get/put method or leak of schema or iter objects."
                );
            }
            let (next_guard, _) = self
                .access_wait_condvar
                .wait_timeout(wait_guard, std::time::Duration::from_millis(100))
                .unwrap();
            wait_guard = next_guard;
            waited += 100;
        }
    }

    fn wait_for_other_accesses_to_drain(&self) {
        if self.active_accesses.load(Ordering::Acquire) == 1 {
            return;
        }
        let mut wait_guard = self.access_wait_mutex.lock().unwrap();
        while self.active_accesses.load(Ordering::Acquire) != 1 {
            wait_guard = self.access_wait_condvar.wait(wait_guard).unwrap();
        }
    }

    fn release_access(&self) {
        if self.active_accesses.fetch_sub(1, Ordering::AcqRel) <= 2 {
            self.access_wait_condvar.notify_all();
        }
    }

    fn error_or_invalid_state(&self) -> Error {
        let state = self.state();
        if state == DbLifecycleState::Error
            && let Some(err) = self.error.load_full()
        {
            return err.as_ref().clone();
        }
        Error::InvalidState(format!("db is {}", state.as_str()))
    }
}

#[cfg(test)]
#[path = "../tests/unit/db_status.rs"]
mod tests;
