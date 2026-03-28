use std::fmt::Debug;
use crate::error::ErrorKind;

pub(crate) struct BufferView<'a> {
    inner: &'a [u8],
    consumed: usize,
}

impl Debug for BufferView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.inner[self.consumed..])
    }
}

impl BufferView<'_> {
    pub fn new(inner: &'_ [u8]) -> BufferView<'_> {
        BufferView { inner, consumed: 0 }
    }

    pub fn consume_into_u32(&mut self) -> Result<u32, ErrorKind> {
        if self.consumed + 4 > self.inner.len() {
            return Err(ErrorKind::IncompleteData);
        }
        let res = u32::from_be_bytes(
            self.inner[self.consumed..self.consumed + 4]
                .try_into()
                .unwrap(),
        );
        self.consumed += 4;
        Ok(res)
    }

    pub fn consume_into_u16(&mut self) -> Result<u16, ErrorKind> {
        if self.consumed + 2 > self.inner.len() {
            return Err(ErrorKind::IncompleteData);
        }
        let res = u16::from_be_bytes(
            self.inner[self.consumed..self.consumed + 2]
                .try_into()
                .unwrap(),
        );
        self.consumed += 2;
        Ok(res)
    }

    pub fn consume_into_vec_n(&mut self, n: usize) -> Result<Vec<u8>, ErrorKind> {
        if self.consumed + n > self.inner.len() {
            return Err(ErrorKind::IncompleteData);
        }
        let data = self.inner[self.consumed..self.consumed + n].to_vec();
        self.consumed += n;
        if data.len() != n {
            return Err(ErrorKind::IncompleteData);
        }
        Ok(data)
    }

    pub fn remaining(&self) -> usize {
        self.inner.len() - self.consumed
    }

    pub fn consumed(&self) -> usize {
        self.consumed
    }

    pub fn swallow(&mut self, n: usize) {
        self.consumed += n;
    }
}

