//! Bounded-memory segment decryption and decoy-header removal.
use std::io::Write;

use aes::cipher::{BlockModeDecrypt, KeyIvInit, block_padding::Pkcs7, inout::InOutBuf};

use crate::jav::util::strip_fake_header;

type Cipher = cbc::Decryptor<aes::Aes128>;
// strip_fake_header searches offsets 0..=8000 and checks five 188-byte packets.
const HEADER_PROBE: usize = 8000 + 188 * 4 + 1;

pub(super) struct SegmentWriter<W> {
    output: W,
    cipher: Option<Cipher>,
    pending: Vec<u8>,
    prefix: Vec<u8>,
    header_done: bool,
}

impl<W: Write> SegmentWriter<W> {
    pub(super) fn new(output: W, encryption: Option<(&[u8], &[u8; 16])>) -> anyhow::Result<Self> {
        let cipher = encryption
            .map(|(key, iv)| {
                anyhow::ensure!(key.len() == 16, "unexpected AES key length {}", key.len());
                Cipher::new_from_slices(key, iv).map_err(|e| anyhow::anyhow!("cipher init: {e}"))
            })
            .transpose()?;
        Ok(Self {
            output,
            cipher,
            pending: Vec::new(),
            prefix: Vec::new(),
            header_done: false,
        })
    }

    pub(super) fn push(&mut self, data: &[u8]) -> anyhow::Result<()> {
        let Some(cipher) = self.cipher.as_mut() else {
            return self.write_plain(data);
        };
        self.pending.extend_from_slice(data);
        // Keep the final block until EOF so PKCS#7 padding is always validated.
        let count = self.pending.len().saturating_sub(1) / 16 * 16;
        if count == 0 {
            return Ok(());
        }
        let mut pending = std::mem::take(&mut self.pending);
        let (blocks, _) = InOutBuf::from(&mut pending[..count]).into_chunks();
        cipher.decrypt_blocks_inout(blocks);
        self.write_plain(&pending[..count])?;
        pending.copy_within(count.., 0);
        pending.truncate(pending.len() - count);
        self.pending = pending;
        Ok(())
    }

    fn write_plain(&mut self, mut data: &[u8]) -> anyhow::Result<()> {
        if !self.header_done {
            let count = data.len().min(HEADER_PROBE - self.prefix.len());
            self.prefix.extend_from_slice(&data[..count]);
            data = &data[count..];
            if self.prefix.len() == HEADER_PROBE || self.prefix.first() == Some(&0x47) {
                self.flush_header()?;
            }
        }
        self.output.write_all(data)?;
        Ok(())
    }

    fn flush_header(&mut self) -> std::io::Result<()> {
        let stripped = strip_fake_header(&self.prefix);
        self.output.write_all(if stripped.is_empty() {
            &self.prefix
        } else {
            stripped
        })?;
        self.prefix.clear();
        self.header_done = true;
        Ok(())
    }

    pub(super) fn finish(mut self) -> anyhow::Result<W> {
        if let Some(cipher) = self.cipher.take() {
            anyhow::ensure!(self.pending.len() == 16, "truncated AES segment");
            let mut pending = std::mem::take(&mut self.pending);
            let plain = cipher
                .decrypt_padded::<Pkcs7>(&mut pending)
                .map_err(|e| anyhow::anyhow!("AES padding: {e}"))?;
            self.write_plain(plain)?;
        }
        anyhow::ensure!(
            self.header_done || !self.prefix.is_empty(),
            "empty decrypted segment"
        );
        if !self.header_done {
            self.flush_header()?;
        }
        self.output.flush()?;
        Ok(self.output)
    }
}
