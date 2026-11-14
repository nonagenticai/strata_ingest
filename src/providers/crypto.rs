//! Tenant provider keys at rest: AES-256-GCM with the 32-byte key from
//! `STRATA_INGEST_PROVIDER_KEY` (base64), a random 96-bit nonce per write, and
//! `tenant || capability` as associated data, so a ciphertext copied to another tenant's or
//! capability's row does not decrypt. Same scheme as strata_rag (`STRATA_PROVIDER_KEY`).

use std::fmt;

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, Generate, KeyInit, Nonce, Payload};
use base64::Engine as _;
use uuid::Uuid;

use super::Capability;
use crate::config::Secret;
use crate::error::{IngestError, Result};

/// The env var holding the sealing key.
pub const KEY_ENV: &str = "STRATA_INGEST_PROVIDER_KEY";
/// `tenant_providers.key_version` of keys sealed by this build.
pub const KEY_VERSION: i32 = 1;
const NONCE_LEN: usize = 12;

/// The sealing key. `Debug` never prints it.
pub struct KeyCipher(Aes256Gcm);

impl fmt::Debug for KeyCipher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KeyCipher(***)")
    }
}

/// A sealed provider key, as stored (`api_key_ct`, `api_key_nonce`).
#[derive(Clone, PartialEq, Eq)]
pub struct Sealed {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
}

impl fmt::Debug for Sealed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sealed")
            .field("ciphertext_len", &self.ciphertext.len())
            .finish_non_exhaustive()
    }
}

fn aad(tenant: Uuid, cap: Capability) -> Vec<u8> {
    let mut v = tenant.as_bytes().to_vec();
    v.extend_from_slice(cap.as_str().as_bytes());
    v
}

/// A store row's associated data (`store_providers`, migration 0008): tenant || store ||
/// capability. 32 uuid bytes before the capability where a tenant row has 16, so no store AAD
/// equals any tenant AAD: a key never opens after a move between the two tables.
fn store_aad(tenant: Uuid, store: Uuid, cap: Capability) -> Vec<u8> {
    let mut v = tenant.as_bytes().to_vec();
    v.extend_from_slice(store.as_bytes());
    v.extend_from_slice(cap.as_str().as_bytes());
    v
}

/// The 32 key bytes of a `STRATA_INGEST_PROVIDER_KEY` value. Errors name the variable, never
/// the value.
pub fn decode_key(raw: &str) -> Result<[u8; 32]> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(raw.trim())
        .map_err(|_| IngestError::Config(format!("{KEY_ENV} is not valid base64")))?;
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| {
        IngestError::Config(format!(
            "{KEY_ENV} must decode to exactly 32 bytes (got {}); generate one with \
             `openssl rand -base64 32`",
            bytes.len()
        ))
    })
}

impl KeyCipher {
    pub fn new(key: &[u8; 32]) -> Self {
        KeyCipher(Aes256Gcm::new(&(*key).into()))
    }

    pub fn from_base64(raw: &str) -> Result<Self> {
        Ok(Self::new(&decode_key(raw)?))
    }

    pub fn seal(&self, tenant: Uuid, cap: Capability, key: &Secret) -> Result<Sealed> {
        self.seal_aad(&aad(tenant, cap), key)
    }

    /// Seals a store-bound provider key (`store_providers`) for exactly `(tenant, store, cap)`.
    pub fn seal_store(
        &self,
        tenant: Uuid,
        store: Uuid,
        cap: Capability,
        key: &Secret,
    ) -> Result<Sealed> {
        self.seal_aad(&store_aad(tenant, store, cap), key)
    }

    /// The key sealed by [`KeyCipher::seal_store`] for exactly `(tenant, store, cap)`.
    pub fn open_store(
        &self,
        tenant: Uuid,
        store: Uuid,
        cap: Capability,
        sealed: &Sealed,
    ) -> Result<Secret> {
        self.open_aad(&store_aad(tenant, store, cap), sealed, || {
            format!(
                "the stored {} provider key of store {store} does not decrypt with {KEY_ENV} \
                 (wrong key, or the row was altered)",
                cap.as_str()
            )
        })
    }

    fn seal_aad(&self, aad: &[u8], key: &Secret) -> Result<Sealed> {
        let nonce = Nonce::<Aes256Gcm>::generate();
        let ciphertext = self
            .0
            .encrypt(
                &nonce,
                Payload {
                    msg: key.expose().as_bytes(),
                    aad,
                },
            )
            .map_err(|_| IngestError::Config("provider key encryption failed".to_string()))?;
        Ok(Sealed {
            ciphertext,
            nonce: nonce.to_vec(),
        })
    }

    /// The key sealed for exactly this `(tenant, cap)`; a wrong key, a tampered row or a row
    /// moved between tenants / capabilities is an error.
    pub fn open(&self, tenant: Uuid, cap: Capability, sealed: &Sealed) -> Result<Secret> {
        self.open_aad(&aad(tenant, cap), sealed, || {
            format!(
                "the stored {} provider key of tenant {tenant} does not decrypt with {KEY_ENV} \
                 (wrong key, or the row was altered)",
                cap.as_str()
            )
        })
    }

    fn open_aad(
        &self,
        aad: &[u8],
        sealed: &Sealed,
        message: impl Fn() -> String,
    ) -> Result<Secret> {
        let fail = || IngestError::Config(message());
        if sealed.nonce.len() != NONCE_LEN {
            return Err(fail());
        }
        let nonce = Nonce::<Aes256Gcm>::try_from(sealed.nonce.as_slice()).map_err(|_| fail())?;
        let plain = self
            .0
            .decrypt(
                &nonce,
                Payload {
                    msg: &sealed.ciphertext,
                    aad,
                },
            )
            .map_err(|_| fail())?;
        String::from_utf8(plain)
            .map(Secret::new)
            .map_err(|_| fail())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    /// A fresh 32-byte key from the OS RNG: no test carries key material in its source.
    fn test_key() -> [u8; 32] {
        <[u8; 32]>::generate()
    }

    #[test]
    fn a_store_key_opens_only_for_its_store_and_never_as_a_tenant_key() {
        let c = KeyCipher::new(&test_key());
        let store = t(77);
        let s = c
            .seal_store(t(1), store, Capability::Ocr, &Secret::new("sk-store"))
            .unwrap();
        assert_eq!(
            c.open_store(t(1), store, Capability::Ocr, &s)
                .unwrap()
                .expose(),
            "sk-store"
        );
        assert!(c.open_store(t(1), t(78), Capability::Ocr, &s).is_err());
        assert!(c.open_store(t(2), store, Capability::Ocr, &s).is_err());
        assert!(c.open_store(t(1), store, Capability::Vision, &s).is_err());
        // moved between the tables, either way
        assert!(c.open(t(1), Capability::Ocr, &s).is_err());
        let tenant_sealed = c.seal(t(1), Capability::Ocr, &Secret::new("k")).unwrap();
        assert!(
            c.open_store(t(1), t(1), Capability::Ocr, &tenant_sealed)
                .is_err()
        );
        let err = c
            .open_store(t(2), store, Capability::Ocr, &s)
            .unwrap_err()
            .to_string();
        assert!(!err.contains("sk-store") && err.contains(KEY_ENV), "{err}");
    }

    #[test]
    fn round_trip_with_a_fresh_nonce_per_write() {
        let c = KeyCipher::new(&test_key());
        let key = Secret::new("sk-round-trip-123");
        let a = c.seal(t(1), Capability::Ocr, &key).unwrap();
        let b = c.seal(t(1), Capability::Ocr, &key).unwrap();
        assert_ne!(a.nonce, b.nonce, "random nonce per write");
        assert_ne!(a.ciphertext, b.ciphertext);
        assert_eq!(a.nonce.len(), NONCE_LEN);
        let needle = b"sk-round-trip-123";
        assert!(!a.ciphertext.windows(needle.len()).any(|w| w == needle));
        assert_eq!(
            c.open(t(1), Capability::Ocr, &a).unwrap().expose(),
            "sk-round-trip-123"
        );
    }

    #[test]
    fn a_ciphertext_moved_to_another_tenant_or_capability_does_not_open() {
        let c = KeyCipher::new(&test_key());
        let s = c.seal(t(1), Capability::Ocr, &Secret::new("k")).unwrap();
        assert!(c.open(t(2), Capability::Ocr, &s).is_err());
        assert!(c.open(t(1), Capability::Vision, &s).is_err());
        assert!(c.open(t(1), Capability::Transcribe, &s).is_err());
        let mut tampered = s.clone();
        tampered.ciphertext[0] ^= 1;
        assert!(c.open(t(1), Capability::Ocr, &tampered).is_err());
        let short = Sealed {
            nonce: vec![0; 4],
            ..s
        };
        assert!(c.open(t(1), Capability::Ocr, &short).is_err());
    }

    #[test]
    fn a_wrong_key_does_not_open_and_the_error_holds_no_secret() {
        let s = KeyCipher::new(&test_key())
            .seal(t(1), Capability::Vision, &Secret::new("sk-wrong-key-456"))
            .unwrap();
        let err = KeyCipher::new(&test_key())
            .open(t(1), Capability::Vision, &s)
            .unwrap_err()
            .to_string();
        assert!(err.contains(KEY_ENV), "{err}");
        assert!(!err.contains("sk-wrong-key-456"), "{err}");
    }

    #[test]
    fn decode_key_checks_base64_and_length_without_echoing() {
        let good = base64::engine::general_purpose::STANDARD.encode([3u8; 32]);
        assert_eq!(decode_key(&format!(" {good} ")).unwrap(), [3u8; 32]);
        let short = base64::engine::general_purpose::STANDARD.encode([3u8; 16]);
        let e = decode_key(&short).unwrap_err().to_string();
        assert!(e.contains("32 bytes") && !e.contains(&short), "{e}");
        let e = decode_key("not base64 at all!!").unwrap_err().to_string();
        assert!(
            e.contains("base64") && !e.contains("not base64 at all"),
            "{e}"
        );
    }

    #[test]
    fn debug_prints_no_key_material() {
        let c = KeyCipher::new(&test_key());
        assert_eq!(format!("{c:?}"), "KeyCipher(***)");
        let s = c.seal(t(1), Capability::Ocr, &Secret::new("k")).unwrap();
        assert!(!format!("{s:?}").contains("nonce: ["), "{s:?}");
    }
}
