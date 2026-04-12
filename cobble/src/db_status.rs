use crate::error::{Error, Result};
use arc_swap::ArcSwapOption;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

const STATE_INITIALIZING: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_CLOSING: u8 = 2;
const STATE_CLOSED: u8 = 3;
const STATE_ERROR: u8 = 4;

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

pub(crate) struct DbLifecycle {
    state: AtomicU8,
    error: ArcSwapOption<Error>,
}

impl DbLifecycle {
    pub(crate) fn new_initializing() -> Self {
        Self {
            state: AtomicU8::new(STATE_INITIALIZING),
            error: ArcSwapOption::empty(),
        }
    }

    pub(crate) fn new_open() -> Self {
        Self {
            state: AtomicU8::new(STATE_OPEN),
            error: ArcSwapOption::empty(),
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
mod tests {
    use super::*;

    #[test]
    fn test_lifecycle_basic_transitions() {
        let lifecycle = DbLifecycle::new_initializing();
        assert_eq!(lifecycle.state(), DbLifecycleState::Initializing);
        lifecycle.mark_open().unwrap();
        assert_eq!(lifecycle.state(), DbLifecycleState::Open);
        assert_eq!(
            lifecycle.begin_close().unwrap(),
            CloseTransition::Transitioned
        );
        assert_eq!(lifecycle.state(), DbLifecycleState::Closing);
        lifecycle.mark_closed();
        assert_eq!(lifecycle.state(), DbLifecycleState::Closed);
        assert_eq!(
            lifecycle.begin_close().unwrap(),
            CloseTransition::AlreadyClosingOrClosed
        );
    }

    #[test]
    fn test_lifecycle_error_holds_original_error() {
        let lifecycle = DbLifecycle::new_initializing();
        let original = Error::IoError("boom".to_string());
        lifecycle.mark_error(original.clone());
        assert_eq!(lifecycle.state(), DbLifecycleState::Error);
        let err = lifecycle.ensure_open().unwrap_err();
        assert_eq!(err.to_string(), original.to_string());
    }
}
